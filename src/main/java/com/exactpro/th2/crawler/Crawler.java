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
import com.exactpro.cradle.intervals.RecoveryState;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataservice.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataservice.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataservice.grpc.DataServiceInfo;
import com.exactpro.th2.crawler.dataservice.grpc.DataServiceService;
import com.exactpro.th2.crawler.dataservice.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataservice.grpc.EventResponse;
import com.exactpro.th2.crawler.dataservice.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataservice.grpc.MessageResponse;
import com.exactpro.th2.crawler.exception.UnexpectedDataServiceException;
import com.exactpro.th2.crawler.exception.ConfigurationException;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl;
import com.exactpro.th2.dataprovider.grpc.*;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Crawler {
    private final DataServiceService dataService;
    private final DataProviderService dataProviderService;
    private final IntervalsWorker intervalsWorker;
    private final CrawlerConfiguration configuration;

    private final Instant from;
    private Instant to;

    private CrawlerTime crawlerTime;

    private final Duration defaultIntervalLength;

    private long numberOfEvents;
    private long numberOfMessages;

    private Set<String> sessionAliases;
    private final Pattern sessionAliasesPattern;

    private final boolean floatingToTime;
    private final boolean workAlone;
    private boolean reachedTo;

    private long sleepTime;

    private final String crawlerType;
    private final int batchSize;
    private final DataServiceInfo info;
    private final CrawlerId crawlerId;

    private static final String EVENTS = "EVENTS";
    private static final String MESSAGES = "MESSAGES";

    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    private Interval interval;

    public Crawler(@NotNull CradleStorage storage, @NotNull DataServiceService dataService,
                   @NotNull DataProviderService dataProviderService, @NotNull CrawlerConfiguration configuration,
                   CrawlerTime crawlerTime) {
        this.intervalsWorker = Objects.requireNonNull(storage, "Cradle storage cannot be null").getIntervalsWorker();
        this.dataService = Objects.requireNonNull(dataService, "Data service cannot be null");
        this.dataProviderService = Objects.requireNonNull(dataProviderService, "Data provider service cannot be null");
        this.configuration = Objects.requireNonNull(configuration, "Crawler configuration cannot be null");
        this.from = Instant.parse(configuration.getFrom());
        this.floatingToTime = configuration.getTo() == null;
        this.workAlone = configuration.getWorkAlone();
        this.crawlerTime = new CrawlerTimeImpl();
        this.to = floatingToTime ? crawlerTime.now() : Instant.parse(configuration.getTo());
        this.defaultIntervalLength = Duration.parse(configuration.getDefaultLength());
        this.numberOfEvents = this.numberOfMessages = 0L;
        this.sleepTime = configuration.getDelay() * 1000;
        this.crawlerType = configuration.getType();
        this.batchSize = configuration.getBatchSize();
        this.crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        info = dataService.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());
        this.sessionAliases = configuration.getSessionAliases() == null ? null : configuration.getSessionAliases();
        this.sessionAliasesPattern = configuration.getSessionAliasesPattern() == null ? null : Pattern.compile(configuration.getSessionAliasesPattern());
        this.crawlerTime = Objects.requireNonNull(crawlerTime, "Crawler time cannot be null");

        prepare();
    }

    public Crawler(@NotNull CradleStorage storage, @NotNull DataServiceService dataService,
                   @NotNull DataProviderService dataProviderService, @NotNull CrawlerConfiguration configuration) {
        this(storage, dataService, dataProviderService, configuration, new CrawlerTimeImpl());
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

    public Duration process() throws IOException, UnexpectedDataServiceException {
        String dataServiceName = info.getName();
        String dataServiceVersion = info.getVersion();

        interval = getOrCreateInterval(dataServiceName, dataServiceVersion, crawlerType);

        if (interval != null) {

            if (!floatingToTime && interval.getEndTime().equals(to))
                reachedTo = true;

            boolean restartPod;
            SendingReport report;

            if (EVENTS.equals(interval.getCrawlerType())) {
                RecoveryState.InnerEvent lastProcessedEvent = interval.getRecoveryState().getLastProcessedEvent();
                EventID startId = null;

                if (lastProcessedEvent != null) {
                    startId = EventUtils.toEventID(lastProcessedEvent.getId());
                }

                report = sendEvents(crawlerId, info, batchSize, startId);

            } else if (MESSAGES.equals(interval.getCrawlerType())) {
                Map<String, RecoveryState.InnerMessage> lastProcessedMessages = interval.getRecoveryState().getLastProcessedMessages();
                Map<String, MessageID> startIds;

                MessageID.Builder builder = MessageID.newBuilder();

                List<MessageID> ids = lastProcessedMessages.values().stream()
                        .map(innerMessage -> {
                            com.exactpro.th2.common.grpc.Direction direction;

                            if (innerMessage.getDirection().equals(Direction.FIRST))
                                direction = com.exactpro.th2.common.grpc.Direction.FIRST;
                            else
                                direction = com.exactpro.th2.common.grpc.Direction.SECOND;

                            return builder.setSequence(innerMessage.getSequence())
                                    .setConnectionId(ConnectionID.newBuilder().setSessionAlias(innerMessage.getSessionAlias()).build())
                                    .setDirection(direction)
                                    .build();
                        })
                        .collect(Collectors.toList());

                startIds = ids.stream().collect(Collectors.toMap(messageID -> messageID.getConnectionId().getSessionAlias(), Function.identity()));

                List<String> newAliases = dataProviderService.getMessageStreams(Empty.getDefaultInstance())
                        .getListStringList().stream()
                        .filter(sessionAliasesPattern.asPredicate())
                        .collect(Collectors.toList());

                boolean foundNewAliases = newAliases.removeAll(sessionAliases);
                sessionAliases.addAll(newAliases);

                if (foundNewAliases) {
                    sendMessages(crawlerId, info, batchSize, null, newAliases, from, interval.getStartTime());
                }

                report = sendMessages(crawlerId, info, batchSize, startIds, sessionAliases, interval.getStartTime(), interval.getEndTime());
            } else {
                throw new ConfigurationException("Type must be either EVENTS or MESSAGES");
            }

            restartPod = report.action == CrawlerAction.STOP;

            if (report.action == CrawlerAction.NONE) {
                interval = intervalsWorker.setIntervalProcessed(interval, true);

                RecoveryState previousState = interval.getRecoveryState();

                if (EVENTS.equals(interval.getCrawlerType())) {
                    RecoveryState state = new RecoveryState(
                            null,
                            previousState.getLastProcessedMessages(),
                            numberOfEvents,
                            previousState.getLastNumberOfMessages());

                    interval = intervalsWorker.updateRecoveryState(interval, state);
                } else if (MESSAGES.equals(interval.getCrawlerType())) {
                    RecoveryState state = new RecoveryState(
                            previousState.getLastProcessedEvent(),
                            null,
                            previousState.getLastNumberOfEvents(),
                            numberOfMessages
                    );

                    interval = intervalsWorker.updateRecoveryState(interval, state);
                }

                numberOfEvents = 0L;
                numberOfMessages = 0L;
            }

            if (restartPod) {
                throw new UnexpectedDataServiceException("Need to restart Crawler because of changed name and/or version of data-service. " +
                        "Old name: "+dataServiceName+", old version: "+dataServiceVersion+". " +
                        "New name: "+report.newName+", new version: "+report.newVersion);
            }

        }

        return Duration.of(configuration.getDelay(), ChronoUnit.SECONDS);
    }


    private SendingReport sendEvents(CrawlerId crawlerId, DataServiceInfo dataServiceInfo, int batchSize, EventID startId) throws IOException {
        EventResponse response;
        EventID resumeId = startId;
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(interval.getStartTime());
        Timestamp toTimestamp = MessageUtils.toTimestamp(interval.getEndTime());

        long diff = 0L;

        String dataServiceName = dataServiceInfo.getName();
        String dataServiceVersion = dataServiceInfo.getVersion();

        while (search) {

            EventDataRequest.Builder dataRequestBuilder = EventDataRequest.newBuilder();
            EventSearchRequest.Builder searchBuilder = EventSearchRequest.newBuilder();
            EventSearchRequest request;

            searchBuilder
                    .setMetadataOnly(BoolValue.newBuilder().setValue(false).build())
                    .setStartTimestamp(fromTimestamp)
                    .setEndTimestamp(toTimestamp)
                    .setResultCountLimit(Int32Value.of(batchSize));

            if (resumeId == null)
                request = searchBuilder.build();
            else
                request = searchBuilder.setResumeFromId(resumeId).build();

            Iterator<StreamResponse> eventsIterator = dataProviderService.searchEvents(request);
            List<EventData> events = new ArrayList<>();

            while (eventsIterator.hasNext()) {
                StreamResponse r = eventsIterator.next();

                if (r.hasEvent()) {
                    EventData event = r.getEvent();

                    if (!event.getStartTimestamp().equals(toTimestamp))
                        events.add(event);
                }
            }

            if (events.isEmpty()) {
                LOGGER.info("No more events in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            EventData lastEvent = events.get(events.size() - 1);

            resumeId = lastEvent.getEventId();

            EventDataRequest eventRequest = dataRequestBuilder.setId(crawlerId).addAllEventData(events).build();

            response = dataService.sendEvent(eventRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshake(crawlerId, dataServiceInfo);
                }
            }

            if (response.hasId()) {
                RecoveryState oldState = interval.getRecoveryState();

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
                    RecoveryState newState = new RecoveryState(
                            event,
                            oldState.getLastProcessedMessages(),
                            numberOfEvents,
                            oldState.getLastNumberOfMessages());

                    interval = intervalsWorker.updateRecoveryState(interval, newState);
                }
            }

            search = events.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, dataServiceName, dataServiceVersion);
    }

    private SendingReport sendMessages(CrawlerId crawlerId, DataServiceInfo dataServiceInfo, int batchSize, Map<String,
            MessageID> startIds, Collection<String> aliases, Instant from, Instant to) throws IOException {

        Map<String, MessageID> resumeIds = startIds;
        MessageResponse response;
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(from);
        Timestamp toTimestamp = MessageUtils.toTimestamp(to);

        long diff = 0L;

        String dataServiceName = dataServiceInfo.getName();
        String dataServiceVersion = dataServiceInfo.getVersion();

        while (search) {

            MessageDataRequest.Builder messageDataBuilder = MessageDataRequest.newBuilder();
            MessageSearchRequest.Builder searchBuilder = MessageSearchRequest.newBuilder();

            MessageSearchRequest request;

            searchBuilder
                    .setStartTimestamp(fromTimestamp)
                    .setEndTimestamp(toTimestamp)
                    .setResultCountLimit(Int32Value.of(batchSize))
                    .setStream(StringList.newBuilder().addAllListString(aliases).build());

            if (resumeIds == null)
                request = searchBuilder.build();
            else
                request = searchBuilder.addAllMessageId(resumeIds.values()).build();

            Iterator<StreamResponse> messagesIterator = dataProviderService.searchMessages(request);

            List<MessageData> messages = new ArrayList<>();

            while (messagesIterator.hasNext()) {
                StreamResponse r = messagesIterator.next();

                if (r.hasMessage()) {
                    MessageData message = r.getMessage();

                    if (!message.getTimestamp().equals(toTimestamp))
                        messages.add(message);
                }
            }

            if (messages.isEmpty()) {
                LOGGER.info("No more messages in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            if (resumeIds != null)
                resumeIds.clear();

            resumeIds = getLastMessagesWithAliases(messages);

            MessageDataRequest messageRequest = messageDataBuilder.setId(crawlerId).addAllMessageData(messages).build();

            response = dataService.sendMessage(messageRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshake(crawlerId, dataServiceInfo);
                }
            }

            if (!response.getIdsMap().isEmpty()) {
                Map<String, MessageID> responseIds = response.getIdsMap();
                RecoveryState oldState = interval.getRecoveryState();

                RecoveryState.InnerMessage message = null;
                Map<String, RecoveryState.InnerMessage> recoveryStateMessages = new HashMap<>();

                long processedMessagesCount = 0L;

                for (MessageID messageID : responseIds.values()) {
                    processedMessagesCount += messages.stream().takeWhile(eventData -> eventData.getMessageId().equals(messageID)).count();
                }

                numberOfMessages += processedMessagesCount + diff;

                diff = batchSize - processedMessagesCount;

                for (MessageData messageData : messages) {

                    String alias = messageData.getMessageId().getConnectionId().getSessionAlias();
                    Instant timestamp = Instant.ofEpochSecond(messageData.getTimestamp().getSeconds(), messageData.getTimestamp().getNanos());
                    Direction direction = Direction.valueOf(messageData.getDirection().toString());
                    long sequence = messageData.getMessageId().getSequence();

                    message = new RecoveryState.InnerMessage(alias, timestamp, direction, sequence);

                    recoveryStateMessages.put(alias, message);
                }

                if (message != null) {
                    Map<String, RecoveryState.InnerMessage> lastProcessedMessages = oldState.getLastProcessedMessages();

                    lastProcessedMessages.putAll(recoveryStateMessages);

                    RecoveryState newState = new RecoveryState(oldState.getLastProcessedEvent(), lastProcessedMessages,
                            oldState.getLastNumberOfEvents(),
                            numberOfMessages);

                    interval = intervalsWorker.updateRecoveryState(interval, newState);
                }
            }

            search = messages.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, dataServiceName, dataServiceVersion);
    }

    private Map<String, MessageID> getLastMessagesWithAliases(List<MessageData> messages) {
        Map<String, MessageID> result = new HashMap<>();

        Set<String> aliases = messages.stream().filter(MessageData::hasMessageId)
                .map(MessageData::getMessageId)
                .map(MessageID::getConnectionId)
                .map(ConnectionID::getSessionAlias)
                .collect(Collectors.toSet());

        for (String alias : aliases) {

            for (MessageData message : messages) {
                if (message.hasMessageId()) {
                    if (alias.equals(message.getMessageId().getConnectionId().getSessionAlias())) {
                        MessageID id = message.getMessageId();
                        result.put(id.getConnectionId().getSessionAlias(), id);

                    }
                }
            }
        }

        return result;
    }

    private SendingReport handshake(CrawlerId crawlerId, DataServiceInfo dataServiceInfo) {
        DataServiceInfo info = dataService.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());

        String dataServiceName = info.getName();
        String dataServiceVersion = info.getVersion();

        if (dataServiceName.equals(dataServiceInfo.getName()) && dataServiceVersion.equals(dataServiceInfo.getVersion())) {
            LOGGER.info("Got the same name ({}) and version ({}) from repeated crawlerConnect", dataServiceName, dataServiceVersion);
            return new SendingReport(CrawlerAction.CONTINUE, dataServiceName, dataServiceVersion);
        } else {
            LOGGER.info("Got another name ({}) or version ({}) from repeated crawlerConnect, restarting component", dataServiceName, dataServiceVersion);
            return new SendingReport(CrawlerAction.STOP, dataServiceName, dataServiceVersion);
        }
    }

    private GetIntervalReport getInterval(Iterable<Interval> intervals) throws IOException {
        Interval lastInterval = null;
        Interval foundInterval = null;
        long intervalsNumber = 0;

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
                if (interval.isProcessed()) {
                    interval = intervalsWorker.setIntervalProcessed(interval, false);
                }

                LOGGER.info("Crawler got interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());

                foundInterval = interval;
            }

            lastInterval = interval;
        }

        LOGGER.info("Crawler retrieved {} intervals from {} to {}", intervalsNumber, from, to);

        return new GetIntervalReport(foundInterval, lastInterval);
    }

    private Interval getOrCreateInterval(String name, String version, String type) throws IOException {

        Instant lagNow = crawlerTime.now().minus(configuration.getToLag(), configuration.getToLagOffsetUnit());

        if (floatingToTime) {
            this.to = lagNow;
        }

        if (lagNow.isBefore(from)) {
            LOGGER.info("Current time with lag: {} is before \"from\" time of Crawler: {}", lagNow, from);
            sleepTime = getSleepTime(lagNow, from);
            return null;
        }

        Iterable<Interval> intervals = intervalsWorker.getIntervals(from, to, name, version, type);

        Duration length = defaultIntervalLength;
        Interval lastInterval;

        GetIntervalReport getReport = getInterval(intervals);

        if (getReport.foundInterval != null)
            return getReport.foundInterval;
        else
            lastInterval = getReport.lastInterval;

        LOGGER.info("Crawler did not find suitable interval. Creating new one if necessary.");

        if (lastInterval != null) {
            Instant lastIntervalEnd = lastInterval.getEndTime();

            if (lastIntervalEnd.isBefore(to)) {

                Instant newIntervalEnd;

                if (lastIntervalEnd.plus(length).isBefore(to)) {

                    newIntervalEnd = lastIntervalEnd.plus(length);

                } else {
                    if (floatingToTime) {

                        sleepTime = getSleepTime(lastIntervalEnd.plus(length), to);

                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Failed to create new interval from: {}, to: {} as it is too early now. Wait for {}",
                                    lastIntervalEnd, lastIntervalEnd.plus(length), Duration.ofMillis(sleepTime));
                        }


                        return null;

                    }
                    newIntervalEnd = to;
                }

                return createAndStoreInterval(lastIntervalEnd, newIntervalEnd, name, version, type, lagNow);
            } else {

                if (!floatingToTime) {
                    LOGGER.info("All intervals between {} and {} were fully processed less than {} {} ago",
                            from, to, configuration.getLastUpdateOffset(), configuration.getLastUpdateOffsetUnit());
                    return null;
                }

                LOGGER.info("Failed to create new interval from: {}, to: {} as the end of the last interval is after " +
                                "end time of Crawler: {}",
                        lastIntervalEnd, lastIntervalEnd.plus(length), to);

                sleepTime = getSleepTime(lastIntervalEnd.plus(length), lagNow); // TODO: we need to start from the beginning I guess

                return null;
            }
        } else {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
        }
    }

    private Interval createAndStoreInterval(Instant from, Instant to, String name, String version, String type, Instant lagTime) throws IOException {

        if (lagTime.isBefore(to)) {
            sleepTime = getSleepTime(lagTime, to);

            LOGGER.info("It is too early now to create new interval from: {}, to: {}. " +
                    "Falling asleep for {} seconds", from, to, sleepTime);

            return null;
        }

        Interval newInterval = Interval.builder()
                .startTime(from)
                .endTime(to)
                .lastUpdateTime(crawlerTime.now())
                .crawlerName(name)
                .crawlerVersion(version)
                .crawlerType(type)
                .processed(false)
                .recoveryState(new RecoveryState(null, null, 0, 0))
                .build();

        boolean intervalStored = intervalsWorker.storeInterval(newInterval);

        if (!intervalStored) {
            LOGGER.info("Failed to store new interval from {} to {}. Trying to get or create an interval again.",
                    from, to);

            sleepTime = 0L; // setting to 0 in order to try again immediately

            return null;
        }

        LOGGER.info("Crawler created interval from: {}, to: {}", newInterval.getStartTime(), newInterval.getEndTime());

        return newInterval;
    }

    private long getSleepTime(Instant from, Instant to) {
        return Duration.between(from, to).abs().toMillis();
    }

    private static class SendingReport {
        private final CrawlerAction action;
        private final String newName;
        private final String newVersion;

        private SendingReport(CrawlerAction action, String newName, String newVersion) {
            this.action = action;
            this.newName = newName;
            this.newVersion = newVersion;
        }
    }

    private static class GetIntervalReport {
        private final Interval foundInterval;
        private final Interval lastInterval;

        private GetIntervalReport(Interval foundInterval, Interval lastInterval) {
            this.foundInterval = foundInterval;
            this.lastInterval = lastInterval;
        }
    }

    private enum CrawlerAction {
        NONE, STOP, CONTINUE
    }
}
