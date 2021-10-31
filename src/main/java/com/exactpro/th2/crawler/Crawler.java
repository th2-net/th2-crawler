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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.exception.ConfigurationException;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.CrawlerUtils.EventsSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Crawler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;
    public static final BinaryOperator<MessageID> EARLIEST_SEQUENCE = (first, second) -> first.getSequence() > second.getSequence() ? second : first;

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
    private final String crawlerType;
    private final int batchSize;
    private final DataProcessorInfo info;
    private final CrawlerId crawlerId;
    private final StateService<RecoveryState> stateService;

    private final Instant from;
    private Instant to;
    private boolean reachedTo;

    private static final String EVENTS = "EVENTS";
    private static final String MESSAGES = "MESSAGES";
    private final MessageSender messageSender;
    private final Handshaker handshaker;

    public Crawler(
            @NotNull StateService<RecoveryState> stateService,
            @NotNull CradleStorage storage,
            @NotNull DataProcessorService dataProcessor,
            @NotNull DataProviderService dataProviderService,
            @NotNull CrawlerConfiguration configuration,
            @NotNull CrawlerTime crawlerTime
    ) {
        this.stateService = requireNonNull(stateService, "'state service' cannot be null");
        this.intervalsWorker = requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.dataProcessor = requireNonNull(dataProcessor, "Data service cannot be null");
        this.dataProviderService = requireNonNull(dataProviderService, "Data provider service cannot be null");
        this.configuration = requireNonNull(configuration, "Crawler configuration cannot be null");
        this.from = Instant.parse(configuration.getFrom());
        this.floatingToTime = configuration.getTo() == null;
        this.workAlone = configuration.getWorkAlone();
        this.to = floatingToTime ? crawlerTime.now() : Instant.parse(configuration.getTo());
        this.defaultIntervalLength = Duration.parse(configuration.getDefaultLength());
        this.defaultSleepTime = configuration.getDelay() * 1000;
        this.crawlerType = configuration.getType();
        this.batchSize = configuration.getBatchSize();
        this.crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        this.info = dataProcessor.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());
        this.sessionAliases = configuration.getSessionAliases();
        this.crawlerTime = requireNonNull(crawlerTime, "Crawler time cannot be null");

        handshaker = new Handshaker(dataProcessor);
        messageSender = new MessageSender(configuration, dataProcessor, dataProviderService, intervalsWorker, stateService, handshaker);

        prepare();
    }

    public Crawler(
            @NotNull StateService<RecoveryState> stateService,
            @NotNull CradleStorage storage,
            @NotNull DataProcessorService dataProcessor,
            @NotNull DataProviderService dataProviderService,
            @NotNull CrawlerConfiguration configuration
    ) {
        this(stateService, storage, dataProcessor, dataProviderService, configuration, new CrawlerTimeImpl());
    }

    private void prepare() {
        if (!(EVENTS.equals(crawlerType) || MESSAGES.equals(crawlerType))) {
            throw new ConfigurationException("Type must be either EVENTS or MESSAGES");
        }

        if (!floatingToTime && Duration.between(from, to).abs().compareTo(defaultIntervalLength) < 0)
            throw new IllegalArgumentException("Distance between \"from\" and \"to\" parameters cannot be less" +
                    "than default length of intervals");

        LOGGER.info("Crawler started working");
    }

    public Duration process() throws IOException, UnexpectedDataProcessorException {
        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        FetchIntervalReport fetchIntervalReport = getOrCreateInterval(dataProcessorName, dataProcessorVersion, crawlerType);

        Interval interval = fetchIntervalReport.interval;

        if (interval != null) {

            reachedTo = !floatingToTime && interval.getEndTime().equals(to);

            SendingReport sendingReport;

            switch (interval.getCrawlerType()) {
                case EVENTS:
                    sendingReport = processEvents(interval, fetchIntervalReport);
                    break;
                case MESSAGES:
                    sendingReport = processMessages(interval, fetchIntervalReport);
                    break;
                default:
                    throw new ConfigurationException("Type must be either EVENTS or MESSAGES");
            }

            handleSendingReport(sendingReport);
        }

        long sleepTime = fetchIntervalReport.sleepTime;

        return Duration.of(sleepTime, ChronoUnit.MILLIS);
    }

    private void handleSendingReport(SendingReport sendingReport) throws IOException, UnexpectedDataProcessorException {
        if (sendingReport.getAction() == CrawlerAction.NONE) {
            Interval interval = sendingReport.getInterval();
            interval = intervalsWorker.setIntervalProcessed(interval, true);

            if (EVENTS.equals(interval.getCrawlerType())) {
                interval = CrawlerUtils.updateEventRecoveryState(intervalsWorker, interval,
                        stateService, sendingReport.getNumberOfEvents());
            } else if (MESSAGES.equals(interval.getCrawlerType())) {
                interval = CrawlerUtils.updateMessageRecoveryState(intervalsWorker, interval,
                        stateService, sendingReport.getNumberOfMessages());
            }

            LOGGER.info("Interval from {}, to {} was processed successfully", interval.getStartTime(), interval.getEndTime());
        }

        if (sendingReport.getAction() == CrawlerAction.STOP) {
            String dataProcessorName = info.getName();
            String dataProcessorVersion = info.getVersion();
            throw new UnexpectedDataProcessorException("Need to restart Crawler because of changed name and/or version of data-service. " +
                    "Old name: "+dataProcessorName+", old version: "+dataProcessorVersion+". " +
                    "New name: "+sendingReport.getNewName()+", new version: "+sendingReport.getNewVersion());
        }
    }

    private SendingReport processMessages(Interval interval, FetchIntervalReport fetchIntervalReport) throws IOException {
        LOGGER.debug("Processing messages...");
        RecoveryState state = stateService.deserialize(interval.getRecoveryState());

        Map<StreamKey, InnerMessageId> lastProcessedMessages = state == null ? null : state.getLastProcessedMessages();

        Map<StreamKey, MessageID> startIds = null;

        if (lastProcessedMessages != null) {
            if (!fetchIntervalReport.processFromStart) {
                startIds = lastProcessedMessages.entrySet().stream()
                        .collect(Collectors.toMap(
                                it -> new StreamKey(it.getKey().getSessionAlias(), it.getKey().getDirection()),
                                it -> {
                                    StreamKey key = it.getKey();
                                    InnerMessageId value = it.getValue();
                                    return MessageID.newBuilder()
                                            .setSequence(value.getSequence())
                                            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(key.getSessionAlias()).build())
                                            .setDirection(key.getDirection())
                                            .build();
                                }
                        ));
            }
        }

        return messageSender.sendMessages(new MessagesInfo(interval, info, startIds,
                sessionAliases, interval.getStartTime(), interval.getEndTime()));
    }

    private SendingReport processEvents(Interval interval, FetchIntervalReport fetchIntervalReport) throws IOException {
        LOGGER.debug("Processing events...");
        InnerEventId lastProcessedEvent;
        RecoveryState state = stateService.deserialize(interval.getRecoveryState());

        lastProcessedEvent = state == null ? null : state.getLastProcessedEvent();

        EventID startId = null;

        if (lastProcessedEvent != null) {
            if (!fetchIntervalReport.processFromStart) {
                startId = EventUtils.toEventID(lastProcessedEvent.getId());
            }
        }

        return sendEvents(new EventsInfo(interval, info, startId,
                interval.getStartTime(), interval.getEndTime()));
    }

    private SendingReport sendEvents(EventsInfo info) throws IOException {
        LOGGER.debug("Sending events...");
        EventResponse response;
        Interval interval = info.interval;
        EventID resumeId = info.startId;
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(info.from);
        Timestamp toTimestamp = MessageUtils.toTimestamp(info.to);
        long numberOfEvents = 0L;

        long diff = 0L;

        String dataProcessorName = info.dataProcessorInfo.getName();
        String dataProcessorVersion = info.dataProcessorInfo.getVersion();

        while (search) {

            EventDataRequest.Builder eventRequestBuilder = EventDataRequest.newBuilder();

            SearchResult<EventData> result = CrawlerUtils.searchEvents(dataProviderService,
                    new EventsSearchParameters(fromTimestamp, toTimestamp, batchSize, resumeId));
            List<EventData> events = result.getData();

            if (events.isEmpty()) {
                LOGGER.info("No more events in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            EventData lastEvent = events.get(events.size() - 1);

            resumeId = lastEvent.getEventId();

            EventDataRequest eventRequest = eventRequestBuilder.setId(crawlerId).addAllEventData(events).build();

            response = dataProcessor.sendEvent(eventRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshaker.handshake(crawlerId, interval, info.dataProcessorInfo, numberOfEvents, 0);
                }
            }

            if (response.hasId()) {
                RecoveryState oldState = stateService.deserialize(interval.getRecoveryState());

                InnerEventId event = null;

                EventID responseId = response.getId();

                long processedEventsCount = events.stream().takeWhile(eventData -> eventData.getEventId().equals(responseId)).count();

                numberOfEvents += processedEventsCount + diff;

                diff = batchSize - processedEventsCount;

                for (EventData eventData: events) {
                    if (eventData.getEventId().equals(responseId)) {
                        Instant startTimeStamp = Instant.ofEpochSecond(eventData.getStartTimestamp().getSeconds(),
                                eventData.getStartTimestamp().getNanos());
                        String id = eventData.getEventId().getId();

                        event = new InnerEventId(startTimeStamp, id);
                        break;
                    }
                }

                if (event != null) {
                    RecoveryState newState;

                    if (oldState == null) {
                        newState = new RecoveryState(
                                event,
                                null,
                                numberOfEvents,
                                0);
                    } else {
                        newState = new RecoveryState(
                                event,
                                oldState.getLastProcessedMessages(),
                                numberOfEvents,
                                oldState.getLastNumberOfMessages());
                    }

                    interval = intervalsWorker.updateRecoveryState(interval, stateService.serialize(newState));
                }
            }

            search = events.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, interval, dataProcessorName, dataProcessorVersion, numberOfEvents, 0);
    }

    private GetIntervalReport getInterval(Iterable<Interval> intervals) throws IOException {
        LOGGER.debug("Getting interval...");

        Interval lastInterval = null;
        Interval foundInterval = null;
        long intervalsNumber = 0;
        boolean processFromStart = true;

        for (Interval interval : intervals) {
            boolean lastUpdateCheck = interval.getLastUpdateDateTime()
                    .isBefore(crawlerTime.now().minus(configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit()));

            intervalsNumber++;

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

        return new GetIntervalReport(foundInterval, lastInterval, processFromStart);
    }

    private FetchIntervalReport getOrCreateInterval(String name, String version, String type) throws IOException {

        LOGGER.debug("Getting interval...");
        Instant lagNow = crawlerTime.now().minus(configuration.getToLag(), configuration.getToLagOffsetUnit());

        if (floatingToTime) {
            this.to = lagNow;
        }

        if (lagNow.isBefore(from)) {
            LOGGER.info("Current time with lag: {} is before \"from\" time of Crawler: {}", lagNow, from);
            return new FetchIntervalReport(null, getSleepTime(lagNow, from), true);
        }

        Iterable<Interval> intervals = intervalsWorker.getIntervals(from, to, name, version, type);

        Duration length = defaultIntervalLength;
        Interval lastInterval;

        GetIntervalReport getReport = getInterval(intervals);

        if (getReport.foundInterval != null) {
            return new FetchIntervalReport(getReport.foundInterval, defaultSleepTime, getReport.processFromStart);
        }

        lastInterval = getReport.lastInterval;

        LOGGER.info("Crawler did not find suitable interval. Creating new one if necessary.");

        if (lastInterval == null) {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
        }

        Instant lastIntervalEnd = lastInterval.getEndTime();

        if (lastIntervalEnd.isBefore(to)) {

            Instant newIntervalEnd;

            if (lastIntervalEnd.plus(length).isBefore(to)) {

                newIntervalEnd = lastIntervalEnd.plus(length);

            } else {
                newIntervalEnd = to;

                if (floatingToTime) {

                    long sleepTime = getSleepTime(lastIntervalEnd, to);

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Failed to create new interval from: {}, to: {} as it is too early now. Wait for {}",
                                lastIntervalEnd,
                                newIntervalEnd,
                                Duration.ofMillis(sleepTime));
                    }

                    return new FetchIntervalReport(null, sleepTime, true);
                }
            }

            return createAndStoreInterval(lastIntervalEnd, newIntervalEnd, name, version, type, lagNow);
        } else {

            if (!floatingToTime) {
                LOGGER.info("All intervals between {} and {} were fully processed less than {} {} ago",
                        from, to, configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit());
                return new FetchIntervalReport(null, defaultSleepTime, true);
            }

            LOGGER.info("Failed to create new interval from: {}, to: {} as the end of the last interval is after " +
                            "end time of Crawler: {}",
                    lastIntervalEnd, lastIntervalEnd.plus(length), to);

            return new FetchIntervalReport(null, getSleepTime(lastIntervalEnd.plus(length), lagNow), true); // TODO: we need to start from the beginning I guess
        }

    }

    private FetchIntervalReport createAndStoreInterval(Instant from, Instant to, String name, String version, String type, Instant lagTime) throws IOException {

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
                .crawlerType(type)
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

    private static class EventsInfo {
        private final Interval interval;
        private final DataProcessorInfo dataProcessorInfo;
        private final EventID startId;
        private final Instant from;
        private final Instant to;

        private EventsInfo(Interval interval, DataProcessorInfo dataProcessorInfo,
                          EventID startId, Instant from, Instant to) {
            this.interval = interval;
            this.dataProcessorInfo = dataProcessorInfo;
            this.startId = startId;
            this.from = from;
            this.to = to;
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
