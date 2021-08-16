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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.state.RecoveryState;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.exception.ConfigurationException;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class Crawler {
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

    private final Instant from;
    private Instant to;
    private boolean reachedTo;

    private static final String EVENTS = "EVENTS";
    private static final String MESSAGES = "MESSAGES";

    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    public Crawler(@NotNull CradleStorage storage, @NotNull DataProcessorService dataProcessor,
                   @NotNull DataProviderService dataProviderService, @NotNull CrawlerConfiguration configuration,
                   CrawlerTime crawlerTime) {
        this.intervalsWorker = Objects.requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.dataProcessor = Objects.requireNonNull(dataProcessor, "Data service cannot be null");
        this.dataProviderService = Objects.requireNonNull(dataProviderService, "Data provider service cannot be null");
        this.configuration = Objects.requireNonNull(configuration, "Crawler configuration cannot be null");
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
        this.crawlerTime = Objects.requireNonNull(crawlerTime, "Crawler time cannot be null");

        prepare();
    }

    public Crawler(@NotNull CradleStorage storage, @NotNull DataProcessorService dataProcessor,
                   @NotNull DataProviderService dataProviderService, @NotNull CrawlerConfiguration configuration) {
        this(storage, dataProcessor, dataProviderService, configuration, new CrawlerTimeImpl());
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

            if (EVENTS.equals(interval.getCrawlerType())) {
                RecoveryState.InnerEvent lastProcessedEvent;
                RecoveryState state = RecoveryState.getStateFromJson(interval.getRecoveryState());

                lastProcessedEvent = state == null ? null : state.getLastProcessedEvent();

                EventID startId = null;

                if (lastProcessedEvent != null) {
                    if (!fetchIntervalReport.processFromStart) {
                        startId = EventUtils.toEventID(lastProcessedEvent.getId());
                    }
                }

                sendingReport = sendEvents(new EventsInfo(interval, info, startId,
                        interval.getStartTime(), interval.getEndTime()));

            } else if (MESSAGES.equals(interval.getCrawlerType())) {
                Map<String, RecoveryState.InnerMessage> lastProcessedMessages;
                RecoveryState state = RecoveryState.getStateFromJson(interval.getRecoveryState());

                lastProcessedMessages = state == null ? null : state.getLastProcessedMessages();

                Map<String, MessageID> startIds = null;

                MessageID.Builder builder = MessageID.newBuilder();

                if (lastProcessedMessages != null) {
                    if (!fetchIntervalReport.processFromStart) {
                        List<MessageID> ids = lastProcessedMessages.values().stream()
                                .map(innerMessage -> {
                                    com.exactpro.th2.common.grpc.Direction direction;

                                    if (innerMessage.getDirection() == Direction.FIRST)
                                        direction = com.exactpro.th2.common.grpc.Direction.FIRST;
                                    else
                                        direction = com.exactpro.th2.common.grpc.Direction.SECOND;

                                    return builder
                                            .setSequence(innerMessage.getSequence())
                                            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(innerMessage.getSessionAlias()).build())
                                            .setDirection(direction)
                                            .build();
                                })
                                .collect(Collectors.toList());

                        startIds = ids.stream().collect(Collectors.toMap(messageID -> messageID.getConnectionId().getSessionAlias(),
                                Function.identity(), (messageID, messageID2) -> messageID2));
                    }
                }

                sendingReport = sendMessages(new MessagesInfo(interval, info, startIds,
                        sessionAliases, interval.getStartTime(), interval.getEndTime()));
            } else {
                throw new ConfigurationException("Type must be either EVENTS or MESSAGES");
            }

            if (sendingReport.action == CrawlerAction.NONE) {
                interval = intervalsWorker.setIntervalProcessed(interval, true);

                RecoveryState previousState = RecoveryState.getStateFromJson(interval.getRecoveryState());

                if (EVENTS.equals(interval.getCrawlerType())) {
                    interval = CrawlerUtils.updateEventRecoveryState(intervalsWorker, interval,
                            previousState, sendingReport.numberOfEvents);
                } else if (MESSAGES.equals(interval.getCrawlerType())) {
                    interval = CrawlerUtils.updateMessageRecoveryState(intervalsWorker, interval,
                            previousState, sendingReport.numberOfMessages);
                }

                LOGGER.info("Interval from {}, to {} was processed successfully", interval.getStartTime(), interval.getEndTime());
            }

            if (sendingReport.action == CrawlerAction.STOP) {
                throw new UnexpectedDataProcessorException("Need to restart Crawler because of changed name and/or version of data-service. " +
                        "Old name: "+dataProcessorName+", old version: "+dataProcessorVersion+". " +
                        "New name: "+sendingReport.newName+", new version: "+sendingReport.newVersion);
            }

        }

        long sleepTime = fetchIntervalReport.sleepTime;

        return Duration.of(sleepTime, ChronoUnit.MILLIS);
    }

    private SendingReport sendEvents(EventsInfo info) throws IOException {
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

            EventSearchRequest.Builder searchBuilder = EventSearchRequest.newBuilder();
            EventDataRequest.Builder eventRequestBuilder = EventDataRequest.newBuilder();

            List<EventData> events = CrawlerUtils.searchEvents(dataProviderService,
                    new CrawlerUtils.EventsSearchInfo(searchBuilder, fromTimestamp, toTimestamp, batchSize, resumeId));

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
                    return handshake(crawlerId, info.dataProcessorInfo, numberOfEvents, 0);
                }
            }

            if (response.hasId()) {
                RecoveryState oldState = RecoveryState.getStateFromJson(interval.getRecoveryState());

                RecoveryState.InnerEvent event = null;

                EventID responseId = response.getId();

                long processedEventsCount = events.stream().takeWhile(eventData -> eventData.getEventId().equals(responseId)).count();

                numberOfEvents += processedEventsCount + diff;

                diff = batchSize - processedEventsCount;

                for (EventData eventData: events) {
                    if (eventData.getEventId().equals(responseId)) {
                        Instant startTimeStamp = Instant.ofEpochSecond(eventData.getStartTimestamp().getSeconds(),
                                eventData.getStartTimestamp().getNanos());
                        String id = eventData.getEventId().getId();

                        event = new RecoveryState.InnerEvent(startTimeStamp, id);
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

                    interval = intervalsWorker.updateRecoveryState(interval, newState.convertToJson());
                }
            }

            search = events.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, dataProcessorName, dataProcessorVersion, numberOfEvents, 0);
    }

    private SendingReport sendMessages(MessagesInfo info) throws IOException {
        Interval interval = info.interval;
        Map<String, MessageID> resumeIds = info.startIds;
        MessageResponse response;
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(info.from);
        Timestamp toTimestamp = MessageUtils.toTimestamp(info.to);
        long numberOfMessages = 0L;

        long diff = 0L;

        String dataProcessorName = info.dataProcessorInfo.getName();
        String dataProcessorVersion = info.dataProcessorInfo.getVersion();

        while (search) {

            MessageSearchRequest.Builder searchBuilder = MessageSearchRequest.newBuilder();
            MessageDataRequest.Builder messageDataBuilder = MessageDataRequest.newBuilder();

            List<MessageData> messages = CrawlerUtils.searchMessages(dataProviderService,
                    new CrawlerUtils.MessagesSearchInfo(searchBuilder, fromTimestamp, toTimestamp, batchSize, resumeIds, info.aliases));

            if (messages.isEmpty()) {
                LOGGER.info("No more messages in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            if (resumeIds != null)
                resumeIds.clear();

            resumeIds = messages.stream()
                    .filter(MessageData::hasMessageId)
                    .map(MessageData::getMessageId)
                    .collect(Collectors.toMap(messageID -> messageID.getConnectionId().getSessionAlias(),
                            Function.identity(), (messageID, messageID2) -> messageID2));

            MessageDataRequest messageRequest = messageDataBuilder.setId(crawlerId).addAllMessageData(messages).build();

            response = dataProcessor.sendMessage(messageRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshake(crawlerId, info.dataProcessorInfo, 0, numberOfMessages);
                }
            }

            if (!response.getIdsMap().isEmpty()) {
                Map<String, MessageID> responseIds = response.getIdsMap();
                RecoveryState oldState = RecoveryState.getStateFromJson(interval.getRecoveryState());

                Map<String, RecoveryState.InnerMessage> recoveryStateMessages;

                long processedMessagesCount = 0L;

                for (MessageID messageID : responseIds.values()) {
                    processedMessagesCount += messages.stream().takeWhile(eventData -> eventData.getMessageId().equals(messageID)).count();
                }

                numberOfMessages += processedMessagesCount + diff;

                diff = batchSize - processedMessagesCount;

                recoveryStateMessages = messages.stream()
                        .map(messageData -> {
                            String alias = messageData.getMessageId().getConnectionId().getSessionAlias();
                            Instant timestamp = Instant.ofEpochSecond(messageData.getTimestamp().getSeconds(), messageData.getTimestamp().getNanos());
                            Direction direction = Direction.valueOf(messageData.getDirection().toString());
                            long sequence = messageData.getMessageId().getSequence();

                            return new RecoveryState.InnerMessage(alias, timestamp, direction, sequence);
                        })
                        .collect(Collectors.toMap(RecoveryState.InnerMessage::getSessionAlias,
                                Function.identity(), (messageID, messageID2) -> messageID2));

                if (!recoveryStateMessages.isEmpty()) {
                    RecoveryState newState;

                    if (oldState == null) {
                        Map<String, RecoveryState.InnerMessage> lastProcessedMessages = new HashMap<>(recoveryStateMessages);

                        newState = new RecoveryState(null, lastProcessedMessages,
                                0,
                                numberOfMessages);
                    } else {
                        Map<String, RecoveryState.InnerMessage> lastProcessedMessages = new HashMap<>();

                        if (oldState.getLastProcessedMessages() != null) {
                            lastProcessedMessages.putAll(oldState.getLastProcessedMessages());
                        }

                        lastProcessedMessages.putAll(recoveryStateMessages);

                        newState = new RecoveryState(oldState.getLastProcessedEvent(), lastProcessedMessages,
                                oldState.getLastNumberOfEvents(),
                                numberOfMessages);
                    }

                    interval = intervalsWorker.updateRecoveryState(interval, newState.convertToJson());
                }
            }

            search = messages.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, dataProcessorName, dataProcessorVersion, 0, numberOfMessages);
    }

    private SendingReport handshake(CrawlerId crawlerId, DataProcessorInfo dataProcessorInfo, long numberOfEvents, long numberOfMessages) {
        DataProcessorInfo info = dataProcessor.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());

        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        if (dataProcessorName.equals(dataProcessorInfo.getName()) && dataProcessorVersion.equals(dataProcessorInfo.getVersion())) {
            LOGGER.info("Got the same name ({}) and version ({}) from repeated crawlerConnect", dataProcessorName, dataProcessorVersion);
            return new SendingReport(CrawlerAction.CONTINUE, dataProcessorName, dataProcessorVersion, numberOfEvents, numberOfMessages);
        } else {
            LOGGER.info("Got another name ({}) or version ({}) from repeated crawlerConnect, restarting component", dataProcessorName, dataProcessorVersion);
            return new SendingReport(CrawlerAction.STOP, dataProcessorName, dataProcessorVersion, numberOfEvents, numberOfMessages);
        }
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

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Interval from Cassandra from {}, to {}", interval.getStartTime(), interval.getEndTime());
            }

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

        if (lastInterval != null) {
            Instant lastIntervalEnd = lastInterval.getEndTime();

            if (lastIntervalEnd.isBefore(to)) {

                Instant newIntervalEnd;

                if (lastIntervalEnd.plus(length).isBefore(to)) {

                    newIntervalEnd = lastIntervalEnd.plus(length);

                } else {
                    newIntervalEnd = to;

                    if (floatingToTime) {

                        long sleepTime = getSleepTime(newIntervalEnd, to);

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
        } else {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
        }
    }

    private FetchIntervalReport createAndStoreInterval(Instant from, Instant to, String name, String version, String type, Instant lagTime) throws IOException {

        long sleepTime = defaultSleepTime;

        if (lagTime.isBefore(to)) {
            sleepTime = getSleepTime(lagTime, to);

            LOGGER.info("It is too early now to create new interval from: {}, to: {}. " +
                    "Falling asleep for {} seconds", from, to, sleepTime);

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
                .recoveryState(new RecoveryState(null, null, 0, 0).convertToJson())
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

    private static class SendingReport {
        private final CrawlerAction action;
        private final String newName;
        private final String newVersion;
        private final long numberOfEvents;
        private final long numberOfMessages;


        private SendingReport(CrawlerAction action, String newName, String newVersion, long numberOfEvents, long numberOfMessages) {
            this.action = action;
            this.newName = newName;
            this.newVersion = newVersion;
            this.numberOfEvents = numberOfEvents;
            this.numberOfMessages = numberOfMessages;
        }
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

    private static class MessagesInfo {
        private final Interval interval;
        private final DataProcessorInfo dataProcessorInfo;
        private final Map<String, MessageID> startIds;
        private final Collection<String> aliases;
        private final Instant from;
        private final Instant to;

        private MessagesInfo(Interval interval, DataProcessorInfo dataProcessorInfo, Map<String,
                MessageID> startIds, Collection<String> aliases, Instant from, Instant to) {
            this.interval = interval;
            this.dataProcessorInfo = dataProcessorInfo;
            this.startIds = startIds;
            this.aliases = aliases;
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

    private enum CrawlerAction {
        NONE, STOP, CONTINUE
    }
}
