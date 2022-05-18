/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.crawler;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BinaryOperator;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.exception.UnsupportedRecoveryStateException;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.Method;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.google.protobuf.Timestamp;

public class Crawler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;

    private final DataProcessorService dataProcessor;
    private final DataProviderService dataProviderService;
    private final IntervalsWorker intervalsWorker;
    private final CrawlerConfiguration configuration;
    private final CrawlerTime crawlerTime;
    private final Duration defaultIntervalLength;
    private final long defaultSleepTime;
    private final Set<String> sessionAliases;
    private final boolean floatingToTime;
    private final boolean workAlone;
    private final DataType crawlerType;
    private final int batchSize;
    private final DataProcessorInfo info;
    private final CrawlerId crawlerId;
    private final StateService<RecoveryState> stateService;
    private final CrawlerMetrics metrics;
    private final DataTypeStrategy<Continuation, DataPart> typeStrategy;

    private final Instant from;
    private Instant to;
    private boolean reachedTo;
    private Instant lastIntervalCompatibilityChecked;

    public Crawler(
            @NotNull StateService<RecoveryState> stateService,
            @NotNull CradleStorage storage,
            @NotNull DataProcessorService dataProcessor,
            @NotNull DataProviderService dataProviderService,
            @NotNull CrawlerConfiguration configuration,
            @NotNull CrawlerContext crawlerContext
    ) {
        this.stateService = requireNonNull(stateService, "'state service' cannot be null");
        this.intervalsWorker = requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.dataProcessor = requireNonNull(dataProcessor, "Data service cannot be null");
        this.dataProviderService = requireNonNull(dataProviderService, "Data provider service cannot be null");
        this.configuration = requireNonNull(configuration, "Crawler configuration cannot be null");
        this.from = Instant.parse(configuration.getFrom());
        this.floatingToTime = configuration.getTo() == null;
        this.workAlone = configuration.getWorkAlone();
        this.crawlerTime = requireNonNull(crawlerContext.getCrawlerTime(), "Crawler time cannot be null");
        this.to = floatingToTime ? crawlerTime.now() : Instant.parse(configuration.getTo());
        this.defaultIntervalLength = Duration.parse(configuration.getDefaultLength());
        this.defaultSleepTime = configuration.getDelay() * 1000;
        this.crawlerType = configuration.getType();
        this.batchSize = configuration.getBatchSize();
        this.crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        this.sessionAliases = configuration.getSessionAliases();
        metrics = requireNonNull(crawlerContext.getMetrics(), "'metrics' must not be null");
        info = crawlerConnect(dataProcessor, CrawlerInfo.newBuilder().setId(crawlerId).build());
        // TODO: overrides value of configuration.maxOutgoingDataSize by info.maxOutgoingDataSize if the info's value is less than configuration
        Map<DataType, DataTypeStrategyFactory<Continuation, DataPart>> knownStrategies = loadStrategies();
        DataTypeStrategyFactory<Continuation, DataPart> factory = requireNonNull(knownStrategies.get(crawlerType),
                () -> "Cannot find factory for type: " + crawlerType + ". Known types: " + knownStrategies.keySet());
        typeStrategy = factory.create(intervalsWorker, dataProviderService, stateService, metrics, configuration);
        prepare();
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private Map<DataType, DataTypeStrategyFactory<Continuation, DataPart>> loadStrategies() {
        return ServiceLoader.load(DataTypeStrategyFactory.class).stream()
                .collect(toUnmodifiableMap(
                        it -> it.get().getDataType(),
                        it -> (DataTypeStrategyFactory<Continuation, DataPart>)it.get()
                ));
    }

    private DataProcessorInfo crawlerConnect(@NotNull DataProcessorService dataProcessor, CrawlerInfo crawlerInfo) {
        DataProcessorInfo info = dataProcessor.crawlerConnect(crawlerInfo);
        metrics.processorMethodInvoked(ProcessorMethod.CRAWLER_CONNECT);
        return info;
    }

    private void prepare() {
        if (!floatingToTime && Duration.between(from, to).abs().compareTo(defaultIntervalLength) < 0) {
            throw new IllegalArgumentException("Distance between \"from\" and \"to\" parameters cannot be less" +
                    "than default length of intervals");
        }

        LOGGER.info("Crawler started working");
    }

    public Duration process() throws IOException, UnexpectedDataProcessorException {
        return metrics.measureTimeWithException(crawlerType, Method.HANDLE_INTERVAL, this::internalProcess);
    }

    private Duration internalProcess() throws IOException, UnexpectedDataProcessorException {
        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        FetchIntervalReport fetchIntervalReport = getOrCreateInterval(dataProcessorName, dataProcessorVersion, crawlerType);

        Interval interval = fetchIntervalReport.interval;

        if (interval != null) {
            metrics.currentInterval(interval);

            reachedTo = !floatingToTime && interval.getEndTime().equals(to);

            DataType crawlerType = DataType.byTypeName(interval.getCrawlerType());
            if (crawlerType != this.crawlerType) {
                throw new IllegalStateException("Unexpected data type in state: " + crawlerType + ". Expected type is " + this.crawlerType);
            }

            InternalInterval currentInt = new InternalInterval(stateService, interval);
            RecoveryState state = currentInt.getState();
            intervalStartForProcessor(dataProcessor, interval, state);

            Continuation continuation = state == null ? null : typeStrategy.continuationFromState(state);
            DataParameters parameters = new DataParameters(crawlerId, sessionAliases);
            CrawlerData<Continuation, DataPart> data;
            Report<Continuation> sendingReport = null;
            long processedElements = 0L;
            Timestamp startTime = toTimestamp(interval.getStartTime());
            Timestamp endTime = toTimestamp(interval.getEndTime());
            do {
                LOGGER.trace("Requesting data for interval");
                data = requestData(startTime, endTime, continuation, parameters);
                var currentData = data;
                long remaining = sendingReport == null ? 0 : sendingReport.getRemainingData();
                while (currentData.hasNext()) {
                    DataPart nextPart = currentData.next();
                    Continuation prevCheckpoint = sendingReport == null ? null : sendingReport.getCheckpoint();
                    sendingReport = typeStrategy.processData(dataProcessor, currentInt, parameters, nextPart, prevCheckpoint);

                    if (sendingReport.getAction() == Action.HANDSHAKE) {
                        LOGGER.info("Handshake from {}:{} received. Stop processing", dataProcessorName, dataProcessorVersion);
                        break;
                    }

                    if (nextPart.getSize() > 0) {
                        metrics.updateProcessedData(crawlerType, nextPart.getSize());
                    }

                    Continuation checkpoint = sendingReport.getCheckpoint();
                    processedElements += sendingReport.getProcessedData() + remaining;
                    if (checkpoint != null) {
                        state = typeStrategy.continuationToState(state, checkpoint, processedElements);
                        currentInt.updateState(state, intervalsWorker);
                    }
                }
                sendingReport = requireNonNullElse(sendingReport, Report.empty());

                if (sendingReport.getAction() == Action.HANDSHAKE) {
                    if (currentData.hasNext()) {
                        LOGGER.info("Finishing data iterator because of handshake");
                        int parts = 0;
                        while (currentData.hasNext()) {
                            DataPart dataPart = currentData.next();
                            parts++;
                        }
                        LOGGER.info("Remaining parts in data iterator: {}", parts);
                    }
                    break; // exit from lopping until all data is processed for the interval
                }

                continuation = data.getContinuation(); // because we can do it only when all data was received from provider
            } while (data.isNeedsNextRequest());

            Action action = sendingReport.getAction();
            switch (action) {
            case HANDSHAKE:
                DataProcessorInfo info = crawlerConnect(dataProcessor, CrawlerInfo.newBuilder().setId(crawlerId).build());
                if (!dataProcessorName.equals(info.getName()) || !dataProcessorVersion.equals(info.getVersion())) {
                    throw new UnexpectedDataProcessorException("Need to restart Crawler because of changed name and/or version of data-service. " +
                            "Old name: " + dataProcessorName + ", old version: " + dataProcessorVersion + ". " +
                            "New name: " + info.getName() + ", new version: " + info.getVersion());
                }
                LOGGER.info("Got the same name ({}) and version ({}) from repeated crawlerConnect", dataProcessorName, dataProcessorVersion);
                break;
            case CONTINUE:
                currentInt.processed(true, intervalsWorker);
                currentInt.updateState(state == null
                                ? new RecoveryState(null, null, processedElements, 0)
                                : new RecoveryState(state.getLastProcessedEvent(), state.getLastProcessedMessages(), processedElements, 0),
                        intervalsWorker
                );
                LOGGER.info("Interval from {}, to {} was processed successfully", interval.getStartTime(), interval.getEndTime());
                break;
            default:
                throw new IllegalStateException("Unsupported report action: " + action);
            }
        }

        long sleepTime = fetchIntervalReport.sleepTime;

        return Duration.of(sleepTime, ChronoUnit.MILLIS);
    }

    private CrawlerData<Continuation, DataPart> requestData(Timestamp startTime, Timestamp endTime, Continuation continuation, DataParameters parameters) {
        return typeStrategy.requestData(startTime, endTime, parameters, continuation);
    }

    private void intervalStartForProcessor(DataProcessorService dataProcessor, Interval interval, RecoveryState state) {
        LOGGER.trace("Notifying about interval start (from {} to {})", interval.getStartTime(), interval.getEndTime());
        var intervalInfoBuilder = IntervalInfo.newBuilder()
                .setStartTime(toTimestamp(interval.getStartTime()))
                .setEndTime(toTimestamp(interval.getEndTime()));
        typeStrategy.setupIntervalInfo(intervalInfoBuilder, state);
        dataProcessor.intervalStart(intervalInfoBuilder.build());
        metrics.processorMethodInvoked(ProcessorMethod.INTERVAL_START);
    }

    private GetIntervalReport getInterval(Iterable<Interval> intervals) throws IOException {
        Interval lastInterval = null;
        Interval foundInterval = null;
        long intervalsNumber = 0;
        boolean processFromStart = true;

        for (Interval interval : intervals) {
            boolean lastUpdateCheck = interval.getLastUpdateDateTime()
                    .isBefore(crawlerTime.now().minus(configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit()));

            intervalsNumber++;

            if (compatibilityCheckRequired(interval)) {
                LOGGER.debug("Checking compatibility for interval from {} to {}", interval.getStartTime(), interval.getEndTime());
                try {
                    stateService.checkStateCompatibility(interval.getRecoveryState());
                    lastIntervalCompatibilityChecked = getTimeForLastCompatibilityCheck(interval);
                } catch (Exception ex) {
                    throw new UnsupportedRecoveryStateException(
                            format("The recovery state on interval from %s to %s incompatible: %s",
                                    interval.getStartTime(), interval.getEndTime(), interval.getRecoveryState()),
                            ex
                    );
                }
            }

            LOGGER.trace("Interval from Cassandra from {}, to {}", interval.getStartTime(), interval.getEndTime());

            boolean floatingAndMultiple = floatingToTime && !workAlone && !interval.isProcessed() && lastUpdateCheck;
            boolean floatingAndAlone = floatingToTime && workAlone && !interval.isProcessed();
            boolean fixedAndMultiple = !floatingToTime && !workAlone && !interval.isProcessed() && lastUpdateCheck;
            boolean fixedAndAlone = !floatingToTime && workAlone && (!interval.isProcessed() || lastUpdateCheck);


            if (foundInterval == null && (reachedTo || floatingToTime) && (floatingAndMultiple || floatingAndAlone || fixedAndMultiple || fixedAndAlone)) {
                processFromStart = interval.isProcessed();

                if (interval.isProcessed()) {
                    interval = intervalsWorker.setIntervalProcessed(interval, false);
                }

                LOGGER.info("Crawler got interval from: {}, to: {} with Recovery state {}",
                        interval.getStartTime(), interval.getEndTime(), interval.getRecoveryState());

                foundInterval = interval;
            }

            lastInterval = interval;
        }

        LOGGER.info("Crawler retrieved {} intervals from {} to {}", intervalsNumber, from, to);
        if (lastInterval != null) {
            LOGGER.info("Last interval: {} - {}; state={}", lastInterval.getStartTime(), lastInterval.getEndTime(), lastInterval.getRecoveryState());
        }

        return new GetIntervalReport(foundInterval, lastInterval, processFromStart);
    }

    private boolean compatibilityCheckRequired(Interval interval) {
        return lastIntervalCompatibilityChecked == null
                || getTimeForLastCompatibilityCheck(interval).compareTo(lastIntervalCompatibilityChecked) > 0;
    }

    private Instant getTimeForLastCompatibilityCheck(Interval interval) {
        return interval.getStartTime();
    }

    private FetchIntervalReport getOrCreateInterval(String name, String version, DataType type) throws IOException {

        Instant lagNow = crawlerTime.now().minus(configuration.getToLag(), configuration.getToLagOffsetUnit());

        if (floatingToTime) {
            this.to = lagNow;
        }

        if (lagNow.isBefore(from)) {
            LOGGER.info("Waiting for start of work. Current time with lag: {} is before \"from\" time of Crawler: {}", lagNow, from);
            return new FetchIntervalReport(null, getSleepTime(lagNow, from), true);
        }

        LOGGER.trace("Requesting intervals from {} to {} for name {} and version {}", from, to, name, version);
        Iterable<Interval> intervals = intervalsWorker.getIntervals(from, to, name, version, type.getTypeName());

        Duration length = defaultIntervalLength;

        LOGGER.trace("Looking for suitable interval...");
        GetIntervalReport getReport = getInterval(intervals);

        if (getReport.foundInterval != null) {
            return new FetchIntervalReport(getReport.foundInterval, defaultSleepTime, getReport.processFromStart);
        }

        Interval lastInterval = getReport.lastInterval;

        LOGGER.info("Crawler did not find suitable interval. Creating new one if necessary.");

        if (lastInterval == null) {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
        }
        Instant lastIntervalEnd = lastInterval.getEndTime();

        Instant expectedEnd = lastIntervalEnd.plus(length);
        if (lastIntervalEnd.isBefore(to)) {

            Instant newIntervalEnd;

            if (expectedEnd.isBefore(to)) {

                newIntervalEnd = expectedEnd;

            } else {
                newIntervalEnd = to;

                if (floatingToTime) {

                    long sleepTime = getSleepTime(expectedEnd, to);

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Failed to create new interval from: {}, to: {} as it is too early now. Wait for {}",
                                lastIntervalEnd,
                                expectedEnd,
                                Duration.ofMillis(sleepTime));
                    }

                    return new FetchIntervalReport(null, sleepTime, true);
                }
            }

            return createAndStoreInterval(lastIntervalEnd, newIntervalEnd, name, version, type, lagNow);
        }

        if (!floatingToTime) {
            LOGGER.info("All intervals between {} and {} were fully processed less than {} {} ago",
                    from, to, configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit());

            metrics.currentInterval(CrawlerUtils.EMPTY);

            return new FetchIntervalReport(null, defaultSleepTime, true);
        }

        LOGGER.info("Failed to create new interval from: {}, to: {} as the end of the last interval is after " +
                        "end time of Crawler: {}",
                lastIntervalEnd, expectedEnd, to);

        return new FetchIntervalReport(null, getSleepTime(expectedEnd, lagNow),
                true); // TODO: we need to start from the beginning I guess
    }

    private FetchIntervalReport createAndStoreInterval(Instant from, Instant to, String name, String version, DataType type, Instant lagTime) throws IOException {

        long sleepTime = defaultSleepTime;

        if (lagTime.isBefore(to)) {
            sleepTime = getSleepTime(lagTime, to);

            LOGGER.info("It is too early now to create new interval from: {}, to: {}. " +
                    "Falling asleep for {} millis", from, to, sleepTime);

            return new FetchIntervalReport(null, sleepTime, true);
        }

        Interval newInterval = Interval.builder()
                .startTime(from)
                .endTime(to)
                .lastUpdateTime(crawlerTime.now())
                .crawlerName(name)
                .crawlerVersion(version)
                .crawlerType(type.getTypeName())
                .processed(false)
                .recoveryState(stateService.serialize(
                        new RecoveryState(null, null, 0, 0)
                ))
                .build();

        boolean intervalStored = intervalsWorker.storeInterval(newInterval);

        if (!intervalStored) {
            LOGGER.info("Failed to store new interval from {} to {}. Trying to get or create an interval again.",
                    from, to);

            return new FetchIntervalReport(null, 0L, true); // setting sleepTime to 0 in order to try again immediately
        }

        LOGGER.info("Crawler created interval from: {}, to: {}", newInterval.getStartTime(), newInterval.getEndTime());

        return new FetchIntervalReport(newInterval, sleepTime, true);
    }

    private long getSleepTime(Instant from, Instant to) {
        return Duration.between(from, to).abs().toMillis();
    }

    private static class GetIntervalReport {
        private final Interval foundInterval;
        private final Interval lastInterval;
        private final boolean processFromStart;

        private GetIntervalReport(Interval foundInterval, Interval lastInterval, boolean processFromStart) {
            this.foundInterval = foundInterval;
            this.lastInterval = lastInterval;
            this.processFromStart = processFromStart;
        }
    }

    private static class FetchIntervalReport {
        private final Interval interval;
        private final long sleepTime;
        private final boolean processFromStart;

        private FetchIntervalReport(Interval interval, long sleepTime, boolean processFromStart) {
            this.interval = interval;
            this.sleepTime = sleepTime;
            this.processFromStart = processFromStart;
        }
    }
}
