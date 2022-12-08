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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.utils.CradleStorageException;
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
import com.exactpro.th2.crawler.state.v2.RecoveryState;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.ServiceLoader;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class Crawler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    private final DataProcessorService dataProcessor;
    private final IntervalsWorker intervalsWorker;
    private final CrawlerConfiguration configuration;
    private final CrawlerTime crawlerTime;
    private final Duration defaultIntervalLength;
    private final long defaultSleepTime;
    private final boolean floatingToTime;
    private final boolean workAlone;
    private final DataType crawlerType;
    private final DataProcessorInfo info;
    private final CrawlerId crawlerId;
    private final StateService<RecoveryState> stateService;
    private final CrawlerMetrics metrics;
    private final DataTypeStrategy<Continuation, DataPart> typeStrategy;

    private final BookId bookId;

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
            @NotNull CrawlerContext crawlerContext,
            @NotNull BookId bookId
    ) {
        this.stateService = requireNonNull(stateService, "'state service' cannot be null");
        this.intervalsWorker = requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.dataProcessor = requireNonNull(dataProcessor, "Data service cannot be null");
        requireNonNull(dataProviderService, "Data provider service cannot be null");
        this.configuration = requireNonNull(configuration, "Crawler configuration cannot be null");
        this.from = Instant.parse(configuration.getFrom());
        this.floatingToTime = configuration.getTo() == null;
        this.workAlone = configuration.getWorkAlone();
        this.crawlerTime = requireNonNull(crawlerContext.getCrawlerTime(), "Crawler time cannot be null");
        this.bookId = requireNonNull(bookId, "Book id cannot be null");
        this.to = floatingToTime ? crawlerTime.now() : Instant.parse(configuration.getTo());
        this.defaultIntervalLength = Duration.parse(configuration.getDefaultLength());
        this.defaultSleepTime = configuration.getDelay() * 1000;
        this.crawlerType = configuration.getType();
        this.crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
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

    public Duration process() throws UnexpectedDataProcessorException, CradleStorageException {
        long start = System.currentTimeMillis();
        try {
            return metrics.measureTimeWithException(crawlerType, Method.HANDLE_INTERVAL, this::internalProcess);
        } finally {
            LOGGER.info("Time spent to process interval: {} mls", System.currentTimeMillis() - start);
        }
    }

    private Duration internalProcess() throws UnexpectedDataProcessorException, CradleStorageException {
        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        FetchIntervalReport fetchIntervalReport = getOrCreateInterval(dataProcessorName, dataProcessorVersion, crawlerType);

        Interval interval = fetchIntervalReport.interval;

        if (interval != null) {
            metrics.currentInterval(interval);

            reachedTo = !floatingToTime && interval.getEnd().equals(to);

            DataType crawlerType = DataType.byTypeName(interval.getCrawlerType());
            if (crawlerType != this.crawlerType) {
                throw new IllegalStateException("Unexpected data type in state: " + crawlerType + ". Expected type is " + this.crawlerType);
            }

            InternalInterval currentInt = new InternalInterval(stateService, interval);
            RecoveryState state = currentInt.getState();
            intervalStartForProcessor(dataProcessor, interval, state);

            Continuation continuation = state == null ? null : typeStrategy.continuationFromState(state);
            DataParameters parameters = new DataParameters(crawlerId);
            Report<Continuation> sendingReport = null;
            long processedElements = 0L;
            Timestamp startTime = toTimestamp(interval.getStart());
            Timestamp endTime = toTimestamp(interval.getEnd());
            LOGGER.trace("Requesting data for interval");
            CrawlerData<Continuation, DataPart> data = requestData(startTime, endTime, continuation, parameters);
            long remaining = 0;
            while (data.hasNext()) {
                DataPart nextPart = data.next();
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
                if (data.hasNext()) {
                    LOGGER.info("Finishing data iterator because of handshake");
                    int parts = 0;
                    while (data.hasNext()) {
                        data.next();
                        parts++;
                    }
                    LOGGER.info("Remaining parts in data iterator: {}", parts);
                }
            }

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
                boolean messages = crawlerType == DataType.MESSAGES;
                currentInt.processed(true, intervalsWorker);
                long lastNumberOfEvents = messages ? 0 : processedElements;
                long lastNumberOfMessages = messages ? processedElements : 0;
                currentInt.updateState(state == null
                                ? new RecoveryState(null, null, lastNumberOfEvents, lastNumberOfMessages)
                                : new RecoveryState(state.getLastProcessedEvent(), state.getLastProcessedMessages(), lastNumberOfEvents, lastNumberOfMessages),
                        intervalsWorker
                );
                LOGGER.info("Interval from {}, to {} was processed successfully", interval.getStart(), interval.getEnd());
                break;
            default:
                throw new IllegalStateException("Unsupported report action: " + action);
            }
            metrics.currentInterval(CrawlerUtils.EMPTY);
        }

        long sleepTime = fetchIntervalReport.sleepTime;

        return Duration.of(sleepTime, ChronoUnit.MILLIS);
    }

    private CrawlerData<Continuation, DataPart> requestData(Timestamp startTime, Timestamp endTime, Continuation continuation, DataParameters parameters) {
        return typeStrategy.requestData(startTime, endTime, parameters, continuation);
    }

    private void intervalStartForProcessor(DataProcessorService dataProcessor, Interval interval, RecoveryState state) {
        LOGGER.trace("Notifying about interval start (from {} to {})", interval.getStart(), interval.getEnd());
        var intervalInfoBuilder = IntervalInfo.newBuilder()
                .setStartTime(toTimestamp(interval.getStart()))
                .setEndTime(toTimestamp(interval.getEnd()));
        typeStrategy.setupIntervalInfo(intervalInfoBuilder, state);
        dataProcessor.intervalStart(intervalInfoBuilder.build());
        metrics.processorMethodInvoked(ProcessorMethod.INTERVAL_START);
    }

    private GetIntervalReport getInterval(Iterable<Interval> intervals) throws CradleStorageException {
        Interval lastInterval = null;
        Interval foundInterval = null;
        long intervalsNumber = 0;

        for (Interval interval : intervals) {
            boolean lastUpdateCheck = interval.getLastUpdate()
                    .isBefore(crawlerTime.now().minus(configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit()));

            intervalsNumber++;

            if (compatibilityCheckRequired(interval)) {
                LOGGER.debug("Checking compatibility for interval from {} to {}", interval.getStart(), interval.getEnd());
                try {
                    stateService.checkStateCompatibility(interval.getRecoveryState());
                    lastIntervalCompatibilityChecked = getTimeForLastCompatibilityCheck(interval);
                } catch (Exception ex) {
                    throw new UnsupportedRecoveryStateException(
                            format("The recovery state on interval from %s to %s incompatible: %s",
                                    interval.getStart(), interval.getEnd(), interval.getRecoveryState()),
                            ex
                    );
                }
            }

            LOGGER.trace("Interval from Cassandra from {}, to {}", interval.getStart(), interval.getEnd());

            boolean floatingAndMultiple = floatingToTime && !workAlone && !interval.isProcessed() && lastUpdateCheck;
            boolean floatingAndAlone = floatingToTime && workAlone && !interval.isProcessed();
            boolean fixedAndMultiple = !floatingToTime && !workAlone && !interval.isProcessed() && lastUpdateCheck;
            boolean fixedAndAlone = !floatingToTime && workAlone && (!interval.isProcessed() || lastUpdateCheck);

            if (!reachedTo && !floatingToTime) {
                LOGGER.info("Reached the end of specified time interval: {}", interval.getEnd());
                reachedTo = interval.getEnd().equals(to);
            }

            if (foundInterval == null && (reachedTo || floatingToTime) && (floatingAndMultiple || floatingAndAlone || fixedAndMultiple || fixedAndAlone)) {
                if (interval.isProcessed()) {
                    interval = intervalsWorker.setIntervalProcessed(interval, false);
                }

                LOGGER.info("Crawler got interval from: {}, to: {} with Recovery state {}",
                        interval.getStart(), interval.getEnd(), interval.getRecoveryState());

                foundInterval = interval;
            }

            lastInterval = interval;
        }

        LOGGER.info("Crawler retrieved {} intervals from {} to {}", intervalsNumber, from, to);
        if (lastInterval != null) {
            LOGGER.info("Last interval: {} - {}; state={}", lastInterval.getStart(), lastInterval.getEnd(), lastInterval.getRecoveryState());
        }

        return new GetIntervalReport(foundInterval, lastInterval);
    }

    private boolean compatibilityCheckRequired(Interval interval) {
        return lastIntervalCompatibilityChecked == null
                || getTimeForLastCompatibilityCheck(interval).compareTo(lastIntervalCompatibilityChecked) > 0;
    }

    private Instant getTimeForLastCompatibilityCheck(Interval interval) {
        return interval.getStart();
    }

    private FetchIntervalReport getOrCreateInterval(String name, String version, DataType type) throws CradleStorageException {

        Instant lagNow = crawlerTime.now().minus(configuration.getToLag(), configuration.getToLagOffsetUnit());

        if (floatingToTime) {
            this.to = lagNow;
        }

        if (lagNow.isBefore(from)) {
            LOGGER.info("Waiting for start of work. Current time with lag: {} is before \"from\" time of Crawler: {}", lagNow, from);
            return new FetchIntervalReport(null, getSleepTime(lagNow, from));
        }

        LOGGER.trace("Requesting intervals from {} to {} for name {} and version {}", from, to, name, version);
        Iterable<Interval> intervals = intervalsWorker.getIntervals(bookId, from, to, name, version, type.getTypeName());

        Duration length = defaultIntervalLength;

        LOGGER.trace("Looking for suitable interval...");
        GetIntervalReport getReport = getInterval(intervals);

        if (getReport.foundInterval != null) {
            return new FetchIntervalReport(getReport.foundInterval, defaultSleepTime);
        }

        Interval lastInterval = getReport.lastInterval;

        LOGGER.info("Crawler did not find suitable interval. Creating new one if necessary.");

        if (lastInterval == null) {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
        }
        Instant lastIntervalEnd = lastInterval.getEnd();

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

                    return new FetchIntervalReport(null, sleepTime);
                }
            }

            return createAndStoreInterval(lastIntervalEnd, newIntervalEnd, name, version, type, lagNow);
        }

        if (!floatingToTime) {
            LOGGER.info("All intervals between {} and {} were fully processed less than {} {} ago",
                    from, to, configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit());
            return new FetchIntervalReport(null, defaultSleepTime);
        }

        LOGGER.info("Failed to create new interval from: {}, to: {} as the end of the last interval is after " +
                        "end time of Crawler: {}",
                lastIntervalEnd, expectedEnd, to);

        return new FetchIntervalReport(null, getSleepTime(expectedEnd, lagNow)
        ); // TODO: we need to start from the beginning I guess
    }

    private FetchIntervalReport createAndStoreInterval(Instant from, Instant to, String name, String version, DataType type, Instant lagTime) throws CradleStorageException {

        long sleepTime = defaultSleepTime;

        if (lagTime.isBefore(to)) {
            sleepTime = getSleepTime(lagTime, to);

            LOGGER.info("It is too early now to create new interval from: {}, to: {}. " +
                    "Falling asleep for {} millis", from, to, sleepTime);

            return new FetchIntervalReport(null, sleepTime);
        }

        Interval newInterval = Interval.builder()
                .setBookId(bookId)
                .setStart(from)
                .setEnd(to)
                .setLastUpdate(crawlerTime.now())
                .setCrawlerName(name)
                .setCrawlerVersion(version)
                .setCrawlerType(type.getTypeName())
                .setProcessed(false)
                .setRecoveryState(stateService.serialize(
                        new RecoveryState(null, null, 0, 0)
                ))
                .build();

        boolean intervalStored = intervalsWorker.storeInterval(newInterval);

        if (!intervalStored) {
            LOGGER.info("Failed to store new interval from {} to {}. Trying to get or create an interval again.",
                    from, to);

            return new FetchIntervalReport(null, 0L); // setting sleepTime to 0 in order to try again immediately
        }

        LOGGER.info("Crawler created interval from: {}, to: {}", newInterval.getStart(), newInterval.getEnd());

        return new FetchIntervalReport(newInterval, sleepTime);
    }

    private long getSleepTime(Instant from, Instant to) {
        return Duration.between(from, to).abs().toMillis();
    }

    private static class GetIntervalReport {
        private final Interval foundInterval;
        private final Interval lastInterval;

        private GetIntervalReport(Interval foundInterval, Interval lastInterval) {
            this.foundInterval = foundInterval;
            this.lastInterval = lastInterval;
        }
    }

    private static class FetchIntervalReport {
        private final Interval interval;
        private final long sleepTime;

        private FetchIntervalReport(Interval interval, long sleepTime) {
            this.interval = interval;
            this.sleepTime = sleepTime;
        }
    }
}
