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
import static com.exactpro.th2.crawler.util.CrawlerUtils.toCompletableFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.AsyncDataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
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
import com.exactpro.th2.crawler.util.CrawlerUtils.GrpcMethodCall;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;

import kotlin.Pair;

public class Crawler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;

    private final Map<ProcessorId, AsyncDataProcessorService> dataProcessorsMap;
    private final IntervalsWorker intervalsWorker;
    private final CrawlerConfiguration configuration;
    private final CrawlerTime crawlerTime;
    private final Duration intervalLength;
    private final Duration defaultSleepTime;
    private final Set<String> sessionAliases;
    private final boolean floatingToTime;
    private final boolean workAlone;
    private final DataType crawlerType;
    private final CrawlerId crawlerId;
    private final StateService<RecoveryState> stateService;
    private final CrawlerMetrics metrics;
    private final DataTypeStrategy<CrawlerData<Continuation>, Continuation> typeStrategy;

    private final Instant from;
    private Instant to;
    private boolean reachedTo;
    private final Map<ProcessorId, Instant> lastIntervalCompatibilityChecked = new HashMap<>();

    public Crawler(
            @NotNull StateService<RecoveryState> stateService,
            @NotNull CradleStorage storage,
            @NotNull List<AsyncDataProcessorService> dataProcessors,
            @NotNull DataProviderService dataProviderService,
            @NotNull CrawlerConfiguration configuration,
            @NotNull CrawlerContext crawlerContext
    ) {
        requireNonNull(dataProcessors, "Data service cannot be null");
        if (dataProcessors.isEmpty()) {
            throw new IllegalArgumentException("at least one data processor must be specified");
        }
        this.stateService = requireNonNull(stateService, "'state service' cannot be null");
        this.intervalsWorker = requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.configuration = requireNonNull(configuration, "Crawler configuration cannot be null");
        this.from = Instant.parse(configuration.getFrom());
        this.floatingToTime = configuration.getTo() == null;
        this.workAlone = configuration.getWorkAlone();
        this.crawlerTime = requireNonNull(crawlerContext.getCrawlerTime(), "Crawler time cannot be null");
        this.to = floatingToTime ? crawlerTime.now() : Instant.parse(configuration.getTo());
        this.intervalLength = Duration.parse(configuration.getDefaultLength());
        this.defaultSleepTime = Duration.ofSeconds(configuration.getDelay());
        this.crawlerType = configuration.getType();
        int batchSize = configuration.getBatchSize();
        this.crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        this.sessionAliases = configuration.getSessionAliases();
        metrics = requireNonNull(crawlerContext.getMetrics(), "'metrics' must not be null");
        this.dataProcessorsMap = crawlerConnect(dataProcessors, CrawlerInfo.newBuilder().setId(crawlerId).build());
        Map<DataType, DataTypeStrategyFactory<CrawlerData<Continuation>, Continuation>> knownStrategies = loadStrategies();
        DataTypeStrategyFactory<CrawlerData<Continuation>, Continuation> factory = requireNonNull(knownStrategies.get(crawlerType),
                () -> "Cannot find factory for type: " + crawlerType + ". Known types: " + knownStrategies.keySet());
        typeStrategy = factory.create(intervalsWorker, dataProviderService, stateService, metrics, configuration);
        validateParameters();
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private Map<DataType, DataTypeStrategyFactory<CrawlerData<Continuation>, Continuation>> loadStrategies() {
        return ServiceLoader.load(DataTypeStrategyFactory.class).stream()
                .collect(toUnmodifiableMap(
                        it -> it.get().getDataType(),
                        it -> (DataTypeStrategyFactory<CrawlerData<Continuation>, Continuation>)it.get()
                ));
    }

    private Map<ProcessorId, AsyncDataProcessorService> crawlerConnect(@NotNull List<AsyncDataProcessorService> dataProcessors, CrawlerInfo crawlerInfo) {
        List<CompletableFuture<Pair<DataProcessorInfo, AsyncDataProcessorService>>> futures = dataProcessors.stream()
                .map(processor -> crawlerConnect(processor, crawlerInfo).thenApply(resp -> new Pair<>(resp, processor)))
                .collect(toList());
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return futures.stream()
                .map(CompletableFuture::join)
                .collect(toUnmodifiableMap(
                        pair -> new ProcessorId(pair.getFirst().getName(), pair.getFirst().getVersion()),
                        Pair::getSecond
                ));
    }

    private CompletableFuture<DataProcessorInfo> crawlerConnect(AsyncDataProcessorService processor, CrawlerInfo crawlerInfo) {
        CompletableFuture<DataProcessorInfo> dataProcessorInfo = toCompletableFuture(processor, crawlerInfo, AsyncDataProcessorService::crawlerConnect);
        return dataProcessorInfo.whenComplete((res, ex) -> {
            if (ex == null) {
                metrics.processorMethodInvoked(ProcessorMethod.CRAWLER_CONNECT);
            }
        });
    }

    private void validateParameters() {
        if (!floatingToTime && Duration.between(from, to).abs().compareTo(intervalLength) < 0) {
            throw new IllegalArgumentException("Distance between \"from\" and \"to\" parameters cannot be less" +
                    "than default length of intervals");
        }

        LOGGER.info("Crawler started working");
    }

    public Duration process() throws IOException, UnexpectedDataProcessorException {

        FetchIntervalsReport fetchIntervalsReport = getOrCreateIntervals(crawlerType);

        Map<ProcessorId, IntervalHolder> intervals = fetchIntervalsReport.getIntervals();
        if (intervals == null || intervals.isEmpty()) {
            return fetchIntervalsReport.getSleepTime();
        }

        Instant intervalStartTime = fetchIntervalsReport.getStart();
        Instant intervalEndTime = fetchIntervalsReport.getEnd();

        metrics.currentInterval(intervalStartTime, intervalEndTime);

        reachedTo = !floatingToTime && intervalEndTime.equals(to);

        Map<ProcessorId, InternalInterval> internalIntervals = intervals.entrySet().stream()
                .collect(toUnmodifiableMap(Entry::getKey, entry -> {
                    Interval interval = entry.getValue().getInterval();
                    return new InternalInterval(stateService, interval);
                }));

        executeAll(
                dataProcessorsMap,
                id -> {
                    InternalInterval internal = requireNonNull(internalIntervals.get(id), () -> "Cannot get internal interval for " + id);
                    boolean fromStart = intervals.get(id).isProcessFromStart();
                    return createIntervalInfo(internal.getOriginal(), fromStart ? null : internal.getState());
                },
                this::notifyIntervalStart
        );

        Continuation continuation = null;
        DataParameters parameters = new DataParameters(crawlerId, sessionAliases);
        Map<ProcessorId, Long> processedEvents = new HashMap<>();
        Timestamp startTime = toTimestamp(intervalStartTime);
        Timestamp endTime = toTimestamp(intervalEndTime);
        Map<ProcessorId, Continuation> continuationMap = new HashMap<>(internalIntervals.size());
        Map<ProcessorId, Report<Continuation>> processorsReports = Collections.emptyMap();
        boolean finished = false;
        do {
            LOGGER.trace("Requesting data for interval {} - {}", intervalStartTime, intervalEndTime);
            CrawlerData<Continuation> data = requestData(startTime, endTime, continuation, parameters);

            var lastReports = processorsReports;
            try (var timer = metrics.startTimer(crawlerType, Method.PROCESS_DATA)) {
                processorsReports = executeAll(dataProcessorsMap, (id, processor) -> {
                    InternalInterval interval = internalIntervals.get(id);
                    CrawlerData<Continuation> filterData = filterData(continuationMap, data, id, interval);

                    try {
                        return data.getHasData()
                                ? typeStrategy.processData(processor, interval, parameters, filterData)
                                : CompletableFuture.completedFuture(requireNonNullElse(lastReports.get(id), Report.empty()));
                    } catch (IOException e) {
                        return ExceptionUtils.rethrow(e); // Maybe move exception to lambda parameters
                    }
                });
            }

            continuation = data.getContinuation();
            Map<ProcessorId, Long> remainingMap = lastReports.entrySet().stream()
                    .collect(toUnmodifiableMap(Entry::getKey, it -> it.getValue().getRemainingData()));

            AggregationReport aggregateData = aggregateData(processorsReports, false);

            if (aggregateData.isHasHandshakeRequest()) {
                break;
            }

            if (data.getHasData()) {
                metrics.updateProcessedData(crawlerType, data.size());
            }

            for (Entry<ProcessorId, Report<Continuation>> entry : processorsReports.entrySet()) {
                ProcessorId id = entry.getKey();
                Report<Continuation> report = entry.getValue();
                processReport(internalIntervals, processedEvents, continuationMap, remainingMap, id, report);
            }
            finished = !data.isNeedsNextRequest();
        } while (!finished);

        AggregationReport report = aggregateData(processorsReports, true);

        if (finished) {
            for (ProcessorId id : report.getContinueExecution()) {
                InternalInterval interval = requireNonNull(internalIntervals.get(id), () -> "no holder for " + id);
                RecoveryState state = interval.getState();
                long processed = processedEvents.getOrDefault(id, 0L);
                interval.processed(true, intervalsWorker);
                RecoveryState newState = typeStrategy.continuationToState(state, null, processed);
                interval.updateState(newState, intervalsWorker);
                LOGGER.info("Interval from {}, to {} was processed successfully", interval.getStartTime(), interval.getEndTime());
            }
        }

        if (report.isHasHandshakeRequest()) {
            Map<ProcessorId, DataProcessorInfo> responses = executeAllFiltered(dataProcessorsMap,
                    id -> report.getRequiresHandshake().contains(id),
                    (id, processor) -> crawlerConnect(processor, CrawlerInfo.newBuilder().setId(crawlerId).build()));
            List<Entry<ProcessorId, DataProcessorInfo>> changedCrawler = responses.entrySet().stream()
                    .filter(entry -> {
                        ProcessorId key = entry.getKey();
                        DataProcessorInfo resp = entry.getValue();
                        return !key.getName().equals(resp.getName()) || !key.getVersion().equals(resp.getVersion());
                    }).collect(toList());
            if (!changedCrawler.isEmpty()) {
                throw new UnexpectedDataProcessorException("Need to restart Crawler because of changed name and/or version of data-service. " +
                        changedCrawler.stream()
                                .map(entry -> "Old: " + entry.getKey() + "; New: " + MessageUtils.toJson(entry.getValue()))
                                .collect(Collectors.joining(";"))
                );
            }
            LOGGER.info("Got the same name and version from repeated crawlerConnect {}", report.getRequiresHandshake());
        }

        CrawlerUtils.resetCurrentInterval(metrics);

        return fetchIntervalsReport.sleepTime;
    }

    private void processReport(
            Map<ProcessorId, InternalInterval> internalIntervals,
            Map<ProcessorId, Long> processedEvents,
            Map<ProcessorId, Continuation> continuationMap,
            Map<ProcessorId, Long> remainingMap,
            ProcessorId id,
            Report<Continuation> report
    ) throws IOException {
        Long processed = processedEvents.merge(id, report.getProcessedData() + remainingMap.getOrDefault(id, 0L), Long::sum);
        InternalInterval interval = requireNonNull(internalIntervals.get(id), () -> "no holder for " + id);
        Continuation checkpoint = report.getCheckpoint();
        if (checkpoint != null) {
            continuationMap.put(id, checkpoint);
            RecoveryState state = typeStrategy.continuationToState(interval.getState(), checkpoint, processed);
            interval.updateState(state, intervalsWorker);
        }
    }

    @NotNull
    private CrawlerData<Continuation> filterData(
            Map<ProcessorId, Continuation> continuationMap,
            CrawlerData<Continuation> data,
            ProcessorId id,
            InternalInterval interval
    ) {
        RecoveryState state = interval.getState();
        LOGGER.trace("Filtering data for processor {}", id);
        // TODO: maybe we should move filtering to a different thread
        return typeStrategy.filterData(data, interval,
                // The state for filtering should be taken from the state or from the previous processing
                continuationMap.computeIfAbsent(id, ignore -> state == null ? null : typeStrategy.stateToContinuation(state))
        );
    }

    private AggregationReport aggregateData(Map<ProcessorId, Report<Continuation>> processorsReports, boolean collectHandshakeRequired) {
        List<ProcessorId> requiresHandshake = collectHandshakeRequired ? new ArrayList<>() : Collections.emptyList();
        List<ProcessorId> continueExecution = collectHandshakeRequired ? new ArrayList<>() : Collections.emptyList();
        boolean hasHandshake = false;
        for (Entry<ProcessorId, Report<Continuation>> entry : processorsReports.entrySet()) {
            ProcessorId id = entry.getKey();
            Report<Continuation> report = entry.getValue();
            if (report.getAction() == Action.HANDSHAKE) {
                hasHandshake = true;
                if (collectHandshakeRequired) {
                    requiresHandshake.add(id);
                } else {
                    break; // no need to iterate over all reports
                }
            } else if (collectHandshakeRequired) {
                continueExecution.add(id);
            }
        }
        return collectHandshakeRequired ? new AggregationReport(requiresHandshake, continueExecution) : new AggregationReport(hasHandshake);
    }

    private <T> Map<ProcessorId, T> executeAllFiltered(
            Map<ProcessorId, AsyncDataProcessorService> serviceMap,
            Predicate<ProcessorId> filter,
            BiFunction<ProcessorId, AsyncDataProcessorService, CompletableFuture<T>> action
    ) {
        Map<ProcessorId, CompletableFuture<T>> futures = serviceMap.entrySet().stream()
                .filter(entry -> filter.test(entry.getKey()))
                .collect(toUnmodifiableMap(Entry::getKey, entry -> action.apply(entry.getKey(), entry.getValue())));
        CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new));
        return futures.entrySet().stream()
                .collect(toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().join()));
    }

    private <T> Map<ProcessorId, T> executeAll(
            Map<ProcessorId, AsyncDataProcessorService> serviceMap,
            BiFunction<ProcessorId, AsyncDataProcessorService, CompletableFuture<T>> action
    ) {
        return executeAllFiltered(serviceMap, id -> true, action);
    }

    private <T, R> Map<ProcessorId, T> executeAll(
            Map<ProcessorId, AsyncDataProcessorService> serviceMap,
            Function<ProcessorId, R> requestSupplier,
            GrpcMethodCall<AsyncDataProcessorService, R, T> method
    ) {
        return executeAll(serviceMap, (id, processor) -> toCompletableFuture(processor, requestSupplier.apply(id), method));
    }

    private <T, R> Map<ProcessorId, T> executeAll(
            Map<ProcessorId, AsyncDataProcessorService> serviceMap,
            Function<ProcessorId, R> requestSupplier,
            BiFunction<AsyncDataProcessorService, R, CompletableFuture<T>> method
    ) {
        return executeAll(serviceMap, (id, processor) -> method.apply(processor, requestSupplier.apply(id)));
    }

    private CrawlerData<Continuation> requestData(Timestamp startTime, Timestamp endTime, Continuation continuation, DataParameters parameters) throws IOException {
        return metrics.measureTime(crawlerType, Method.REQUEST_DATA, () -> typeStrategy.requestData(startTime, endTime, parameters, continuation));
    }

    private CompletableFuture<Empty> notifyIntervalStart(AsyncDataProcessorService dataProcessor, IntervalInfo intervalInfo) {
        CompletableFuture<Empty> future = toCompletableFuture(dataProcessor, intervalInfo, AsyncDataProcessorService::intervalStart);
        metrics.processorMethodInvoked(ProcessorMethod.INTERVAL_START);
        return future;
    }

    @NotNull
    private IntervalInfo createIntervalInfo(Interval interval, RecoveryState state) {
        var intervalInfoBuilder = IntervalInfo.newBuilder()
                .setStartTime(toTimestamp(interval.getStartTime()))
                .setEndTime(toTimestamp(interval.getEndTime()));
        typeStrategy.setupIntervalInfo(intervalInfoBuilder, state);
        return intervalInfoBuilder.build();
    }

    private GetIntervalReport getInterval(ProcessorId processorId, Iterable<Interval> intervals) throws IOException {
        Interval lastInterval = null;
        Interval foundInterval = null;
        Interval firstInterval = null;
        long intervalsNumber = 0;
        boolean processFromStart = true;

        for (Interval interval : intervals) {
            if (firstInterval == null) {
                firstInterval = interval;
            }
            boolean lastUpdateCheck = interval.getLastUpdateDateTime()
                    .isBefore(crawlerTime.now().minus(configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit()));

            intervalsNumber++;

            if (compatibilityCheckRequired(processorId, interval)) {
                LOGGER.debug("Checking compatibility for interval from {} to {}", interval.getStartTime(), interval.getEndTime());
                try {
                    stateService.checkStateCompatibility(interval.getRecoveryState());
                    lastIntervalCompatibilityChecked.put(processorId, getTimeForLastCompatibilityCheck(interval));
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

        return new GetIntervalReport(foundInterval, lastInterval, processFromStart, firstInterval);
    }

    private boolean compatibilityCheckRequired(ProcessorId processorId, Interval interval) {
        Instant lastCheck = lastIntervalCompatibilityChecked.get(processorId);
        return lastCheck == null
                || getTimeForLastCompatibilityCheck(interval).compareTo(lastCheck) > 0;
    }

    private Instant getTimeForLastCompatibilityCheck(Interval interval) {
        return interval.getStartTime();
    }

    private FetchIntervalsReport getOrCreateIntervals(DataType type) throws IOException {

        Instant lagNow = crawlerTime.now().minus(configuration.getToLag(), configuration.getToLagOffsetUnit());

        if (floatingToTime) {
            this.to = lagNow;
        }

        if (lagNow.isBefore(from)) {
            LOGGER.info("Waiting for start of work. Current time with lag: {} is before \"from\" time of Crawler: {}", lagNow, from);
            return new FetchIntervalsReport(getSleepTime(lagNow, from));
        }

        Map<ProcessorId, GetIntervalReport> reports = new HashMap<>(dataProcessorsMap.size());
        for (ProcessorId processorId : dataProcessorsMap.keySet()) {

            String name = processorId.getName();
            String version = processorId.getVersion();

            LOGGER.trace("Requesting intervals from {} to {} for name {} and version {}", from, to, name, version);
            Iterable<Interval> intervals = intervalsWorker.getIntervals(from, to, name, version, type.getTypeName());

            LOGGER.trace("Looking for suitable interval...");
            GetIntervalReport report = getInterval(processorId, intervals);
            reports.put(processorId, report);
        }

        IntervalReports intervalReports = checkAndFilterReports(reports, from, floatingToTime ? null : to);

        Instant start = intervalReports.getStart();
        Instant end = intervalReports.getEnd();

        // The estimated end time is in the future
        if (end.isAfter(lagNow)) {
            Duration sleepTime = getSleepTime(end, lagNow);
            LOGGER.info("It is too early now to create new interval from: {}, to: {}. " +
                    "Falling asleep for {} millis", start, end, sleepTime);
            return new FetchIntervalsReport(sleepTime);
        }

        if (!end.isAfter(to)) {
            Map<ProcessorId, IntervalHolder> intervals = new HashMap<>(intervalReports.getReports().size());

            for (Entry<ProcessorId, GetIntervalReport> entry : intervalReports.getReports().entrySet()) {
                ProcessorId id = entry.getKey();
                GetIntervalReport report = entry.getValue();
                if (report.foundInterval != null) {
                    intervals.put(id, new IntervalHolder(checkType(report.foundInterval), report.processFromStart));
                    continue;
                }

                Interval lastInterval = report.lastInterval;
                LOGGER.info("Crawler did not find suitable interval for {}. Creating new one if necessary.", id);
                Instant actualStartTime = lastInterval == null ? start : lastInterval.getEndTime();
                Interval createdInterval = createAndStoreInterval(actualStartTime, end, id.getName(), id.getVersion(), type);
                if (createdInterval == null) {
                    return new FetchIntervalsReport(Duration.ZERO); // immediately try to repeat the procedure
                }
                intervals.put(id, new IntervalHolder(createdInterval, true));
            }
            return new FetchIntervalsReport(start, end, intervals, Duration.ZERO);
        }

        if (!floatingToTime) {
            LOGGER.info("All intervals between {} and {} were fully processed less than {} {} ago",
                    from, to, configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit());
            return new FetchIntervalsReport(defaultSleepTime);
        }

        // TODO: should be checked when searching intervals
        LOGGER.info("Failed to create new interval from: {}, to: {} as the end of the last interval is after " +
                        "end time of Crawler: {}",
                start, end, to);

        return new FetchIntervalsReport(getSleepTime(end.plus(intervalLength), lagNow)); // TODO: we need to start from the beginning I guess
    }

    private Interval checkType(Interval interval) {
        DataType crawlerType = DataType.byTypeName(interval.getCrawlerType());
        if (crawlerType != this.crawlerType) {
            throw new IllegalStateException("Unexpected data type in state: " + crawlerType + ". Expected type is " + this.crawlerType);
        }
        return interval;
    }

    @NotNull
    private IntervalReports checkAndFilterReports(Map<ProcessorId, GetIntervalReport> reports, Instant minBoundary, Instant maxBoundary) {
        Function<Instant, Instant> computeMaxBoundary = original -> maxBoundary == null ? original : ObjectUtils.min(original, maxBoundary);
        Function<GetIntervalReport, Instant> nextIntervalStartTime = rpt -> requireNonNullElse(rpt.getNextIntervalStartTime(), minBoundary);
        // Looking for minimal interval start
        // We need it to compute boundaries for the next data request
        Instant intervalStartTime = reports.values().stream()
                .map(nextIntervalStartTime)
                .map(start -> findClosestPlanningInterval(start, intervalLength, minBoundary))
                .min(Comparator.naturalOrder())
                .orElse(minBoundary);
        Instant intervalEndTime = computeMaxBoundary.apply(intervalStartTime.plus(intervalLength));

        Map<ProcessorId, GetIntervalReport> filtered = new HashMap<>();
        for (Entry<ProcessorId, GetIntervalReport> entry : reports.entrySet()) {
            ProcessorId id = entry.getKey();
            GetIntervalReport report = entry.getValue();

            // Validate that the intervals are aligned in the beginning
            Instant firstIntervalTime = requireNonNullElse(getStartTime(report.firstInterval), minBoundary);
            if (firstIntervalTime.compareTo(minBoundary) != 0) {
                throw new IllegalStateException("The processor " + id + " has first interval with time "
                        + firstIntervalTime + " that does not match the min interval time " + minBoundary);
            }

            // Checking if the suitable interval is aligned.
            Instant startTime = nextIntervalStartTime.apply(report);

            // Find the closes to the suitable interval start and end times to check if it is in boundaries.
            // Interval suites only if it is not yet created or the end time of the created interval matches the potential end time
            Instant closestStart = findClosestPlanningInterval(startTime, intervalLength, intervalStartTime);
            Instant potentialEndTime = computeMaxBoundary.apply(closestStart.plus(intervalLength));
            Instant endTime = requireNonNullElse(report.getNextIntervalEndTime(), potentialEndTime);
            if (report.hasIntervalToProcess() && endTime.compareTo(potentialEndTime) != 0) {
                throw new IllegalStateException("The interval for processor " + id + " is not aligned with current step " + intervalLength);
            }
            if (isBetween(startTime, intervalStartTime, intervalEndTime)) {
                filtered.put(id, report);
            }
        }
        return new IntervalReports(intervalStartTime, intervalEndTime, filtered);
    }

    /**
     * Returns {@code true} if {@code time} is in range [{@code start}; {@code end})
     */
    private static boolean isBetween(Instant time, Instant start, Instant end) {
        return !time.isBefore(start) && time.isBefore(end);
    }

    private static Instant findClosestPlanningInterval(Instant startTime, Duration length, Instant from) {
        Instant closestStart = from;
        Instant next = closestStart;

        do {
            closestStart = next;
            next = closestStart.plus(length);
        } while (!next.isAfter(startTime));
        return closestStart;
    }

    @Nullable
    private static Instant getStartTime(@Nullable Interval interval) {
        return interval == null ? null : interval.getStartTime();
    }

    @Nullable
    private static Instant getEndTime(@Nullable Interval interval) {
        return interval == null ? null : interval.getEndTime();
    }

    private @Nullable Interval createAndStoreInterval(
            Instant from, Instant to,
            String name, String version, DataType type
    ) throws IOException {

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
            LOGGER.warn("Failed to store new interval from {} to {}. Trying to get or create an interval again.", from, to);
            return null;
        }

        LOGGER.info("Crawler created interval from: {}, to: {}", newInterval.getStartTime(), newInterval.getEndTime());

        return newInterval;
    }

    private Duration getSleepTime(Instant from, Instant to) {
        return Duration.between(from, to).abs();
    }

    private static class GetIntervalReport {
        private final Interval foundInterval;
        private final Interval lastInterval;
        private final Interval firstInterval;
        private final boolean processFromStart;

        private GetIntervalReport(Interval foundInterval, Interval lastInterval, boolean processFromStart, Interval firstInterval) {
            this.foundInterval = foundInterval;
            this.lastInterval = lastInterval;
            this.processFromStart = processFromStart;
            this.firstInterval = firstInterval;
        }

        @Nullable
        public Instant getNextIntervalStartTime() {
            Instant startTime = getStartTime(foundInterval);
            return startTime == null ? getEndTime(lastInterval) : startTime;
        }

        @Nullable
        public Instant getNextIntervalEndTime() {
            return getEndTime(foundInterval);
        }

        public boolean hasIntervalToProcess() {
            return foundInterval != null;
        }
    }

    private static class IntervalReports {
        private final Instant start;
        private final Instant end;
        private final Map<ProcessorId, GetIntervalReport> reports;

        private IntervalReports(
                Instant start,
                Instant end,
                Map<ProcessorId, GetIntervalReport> reports
        ) {
            this.start = requireNonNull(start, "'Start' parameter");
            this.end = requireNonNull(end, "'End' parameter");
            this.reports = requireNonNull(reports, "'Reports' parameter");
        }

        public Instant getStart() {
            return start;
        }

        public Instant getEnd() {
            return end;
        }

        public Map<ProcessorId, GetIntervalReport> getReports() {
            return reports;
        }
    }

    private static class IntervalHolder {
        private final Interval interval;
        private final boolean processFromStart;

        IntervalHolder(Interval interval, boolean processFromStart) {
            this.interval = requireNonNull(interval, "'Interval' parameter");
            this.processFromStart = processFromStart;
        }

        public Interval getInterval() {
            return interval;
        }

        public boolean isProcessFromStart() {
            return processFromStart;
        }
    }

    private static class FetchIntervalsReport {
        private final Instant start;
        private final Instant end;
        private final Map<ProcessorId, IntervalHolder> intervals;
        private final Duration sleepTime;

        private FetchIntervalsReport(Duration sleepTime) {
            this(null, null, null, sleepTime);
        }

        private FetchIntervalsReport(Instant start, Instant end, Map<ProcessorId, IntervalHolder> intervals, Duration sleepTime) {
            this.start = start;
            this.end = end;
            this.intervals = intervals;
            this.sleepTime = sleepTime;
        }

        public Instant getStart() {
            return start;
        }

        public Instant getEnd() {
            return end;
        }

        public Map<ProcessorId, IntervalHolder> getIntervals() {
            return intervals;
        }

        public Duration getSleepTime() {
            return sleepTime;
        }
    }

    private static class ProcessorId {
        private final String name;
        private final String version;

        private ProcessorId(String name, String version) {
            this.name = requireNonNull(name, "'Name' parameter");
            this.version = requireNonNull(version, "'Version' parameter");
            if (name.isBlank()) {
                throw new IllegalArgumentException("name cannot be blank");
            }
            if (version.isBlank()) {
                throw new IllegalArgumentException("version cannot be blank");
            }
        }

        public String getName() {
            return name;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ProcessorId that = (ProcessorId)obj;
            return name.equals(that.name) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "[", "]")
                    .add("name='" + name + "'")
                    .add("version='" + version + "'")
                    .toString();
        }
    }

    private static class AggregationReport {
        private final List<ProcessorId> requiresHandshake;
        private final List<ProcessorId> continueExecution;
        private final boolean hasHandshakeRequest;

        public AggregationReport(boolean hasHandshakeRequest) {
            this(List.of(), hasHandshakeRequest, List.of());
        }

        public AggregationReport(List<ProcessorId> requiresHandshake, List<ProcessorId> continueExecution) {
            this(requiresHandshake, !requiresHandshake.isEmpty(), continueExecution);
        }

        private AggregationReport(
                List<ProcessorId> requiresHandshake,
                boolean hasHandshakeRequest,
                List<ProcessorId> continueExecution
        ) {
            this.requiresHandshake = requiresHandshake;
            this.hasHandshakeRequest = hasHandshakeRequest;
            this.continueExecution = continueExecution;
        }

        public List<ProcessorId> getRequiresHandshake() {
            return requiresHandshake;
        }

        public boolean isHasHandshakeRequest() {
            return hasHandshakeRequest;
        }

        public List<ProcessorId> getContinueExecution() {
            return continueExecution;
        }
    }
}
