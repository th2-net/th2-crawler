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
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.exception.ConfigurationException;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.CrawlerUtils.EventsSearchParameters;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;


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

            if (EVENTS.equals(interval.getCrawlerType())) {
                InnerEventId lastProcessedEvent;
                RecoveryState state = stateService.deserialize(interval.getRecoveryState());

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

                sendingReport = sendMessages(new MessagesInfo(interval, info, startIds,
                        sessionAliases, interval.getStartTime(), interval.getEndTime()));
            } else {
                throw new ConfigurationException("Type must be either EVENTS or MESSAGES");
            }

            if (sendingReport.action == CrawlerAction.NONE) {
                interval = sendingReport.interval;
                interval = intervalsWorker.setIntervalProcessed(interval, true);

                if (EVENTS.equals(interval.getCrawlerType())) {
                    interval = CrawlerUtils.updateEventRecoveryState(intervalsWorker, interval,
                            stateService, sendingReport.numberOfEvents);
                } else if (MESSAGES.equals(interval.getCrawlerType())) {
                    interval = CrawlerUtils.updateMessageRecoveryState(intervalsWorker, interval,
                            stateService, sendingReport.numberOfMessages);
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
                    return handshake(crawlerId, interval, info.dataProcessorInfo, numberOfEvents, 0);
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

    private SendingReport sendMessages(MessagesInfo info) throws IOException {
        Interval interval = info.interval;
        MessageResponse response;
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(info.from);
        Timestamp toTimestamp = MessageUtils.toTimestamp(info.to);
        Map<StreamKey, MessageID> resumeIds = info.startIds;
        Map<StreamKey, InnerMessageId> startIDs = toInnerMessageIDs(resumeIds == null ? initialStartIds(fromTimestamp, info.aliases) : resumeIds);
        LOGGER.debug("Start IDs for interval: {}", startIDs);
        long numberOfMessages = 0L;

        long diff = 0L;

        String dataProcessorName = info.dataProcessorInfo.getName();
        String dataProcessorVersion = info.dataProcessorInfo.getVersion();

        while (search) {

            MessageDataRequest.Builder messageDataBuilder = MessageDataRequest.newBuilder();

            MessagesSearchParameters searchParams = MessagesSearchParameters.builder().setFrom(fromTimestamp).setTo(toTimestamp).setBatchSize(batchSize).setResumeIds(resumeIds).setAliases(info.aliases).build();
            SearchResult<MessageData> result = CrawlerUtils.searchMessages(dataProviderService, searchParams);
            List<MessageData> messages = result.getData();

            if (messages.isEmpty()) {
                LOGGER.info("No more messages in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            MessageDataRequest messageRequest = messageDataBuilder.setId(crawlerId).addAllMessageData(messages).build();

            response = dataProcessor.sendMessage(messageRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshake(crawlerId, interval, info.dataProcessorInfo, 0, numberOfMessages);
                }
            }

            List<MessageID> responseIds = response.getIdsList();
            Map.Entry<Integer, Map<StreamKey, InnerMessageId>> processedResult = processServiceResponse(responseIds, result);

            int processedMessagesCount = processedResult == null ? messages.size() : processedResult.getKey();
            numberOfMessages += processedMessagesCount + diff;
            diff = messages.size() - processedMessagesCount;

            if (response.getIdsCount() > 0) {
                requireNonNull(processedResult, () -> "processServiceResponse cannot be null for not empty IDs in response: " + responseIds);
                Map<StreamKey, InnerMessageId> checkpoints = processedResult.getValue();

                if (!checkpoints.isEmpty()) {

                    RecoveryState newState;
                    RecoveryState oldState = stateService.deserialize(interval.getRecoveryState());

                    if (oldState == null) {
                        Map<StreamKey, InnerMessageId> startIntervalIDs = new HashMap<>(startIDs);
                        putAndCheck(checkpoints, startIntervalIDs);

                        newState = new RecoveryState(null, startIntervalIDs, 0, numberOfMessages);
                    } else {
                        Map<StreamKey, InnerMessageId> old = oldState.getLastProcessedMessages();
                        Map<StreamKey, InnerMessageId> lastProcessedMessages = old == null ? new HashMap<>(startIDs) : new HashMap<>(old);
                        putAndCheck(checkpoints, lastProcessedMessages);

                        newState = new RecoveryState(oldState.getLastProcessedEvent(), lastProcessedMessages,
                                oldState.getLastNumberOfEvents(),
                                numberOfMessages);
                    }

                    String recoveryStateJson = stateService.serialize(newState);
                    LOGGER.debug("Recovery state is updated: {}", recoveryStateJson);
                    interval = intervalsWorker.updateRecoveryState(interval, recoveryStateJson);
                }
            }

            resumeIds = requireNonNull(result.getStreamsInfo(), "response from data provider does not have the StreamsInfo")
                    .getStreamsList()
                    .stream()
                    .collect(toUnmodifiableMap(
                            stream -> new StreamKey(stream.getSession(), stream.getDirection()),
                            Stream::getLastId
                    ));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("New resume ids: {}", resumeIds.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + MessageUtils.toJson(entry.getValue()))
                        .collect(Collectors.joining(",")));
            }

            search = messages.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, interval, dataProcessorName, dataProcessorVersion, 0, numberOfMessages);
    }

    private void putAndCheck(Map<StreamKey, InnerMessageId> checkpoints, Map<StreamKey, InnerMessageId> destination) {
        for (Map.Entry<StreamKey, InnerMessageId> entry : checkpoints.entrySet()) {
            var streamKey = entry.getKey();
            var innerMessageId = entry.getValue();
            InnerMessageId prevInnerMessageId = destination.put(streamKey, innerMessageId);
            if (prevInnerMessageId != null && prevInnerMessageId.getSequence() > innerMessageId.getSequence()) {
                LOGGER.warn("The new checkpoint ID {} has less sequence than the previous one {} for stream key {}",
                        innerMessageId.getSequence(), prevInnerMessageId.getSequence(), streamKey);
            }
        }
    }

    private Map<StreamKey, MessageID> initialStartIds(Timestamp fromTimestamp, Collection<String> aliases) {
        int batchSize = 1;
        var parameters = MessagesSearchParameters.builder()
                .setFrom(fromTimestamp)
                .setTo(fromTimestamp)
                .setBatchSize(batchSize)
                .setAliases(aliases)
                .build();
        SearchResult<MessageData> searchResult = CrawlerUtils.searchMessages(dataProviderService, parameters);
        SearchResult<MessageData> oppositeRequest = null;

        if (!searchResult.getData().isEmpty()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Initialising start IDs request has returned unexpected data: " + searchResult.getData().stream()
                        .map(MessageUtils::toJson).collect(Collectors.joining(", ")));
                // We have a message with timestamp equal the `fromTimestamp`
                // Because of that we need to make a request in opposite direction
                // and select the first message for each pair alias + direction that were in the response (should be a single message)
                oppositeRequest = CrawlerUtils.searchMessages(dataProviderService, MessagesSearchParameters.builder()
                        .setFrom(fromTimestamp)
                        .setBatchSize(batchSize)
                        .setResumeIds(associateWithStreamKey(searchResult.getData().stream().map(MessageData::getMessageId), EARLIEST_SEQUENCE))
                        .setAliases(aliases)
                        .setTimeRelation(TimeRelation.PREVIOUS)
                        .build());
            }
        }
        return concat(
                requireNonNull(searchResult.getStreamsInfo(), "stream info is null for initial start IDs response")
                        .getStreamsList().stream()
                        .map(Stream::getLastId),
                oppositeRequest == null ? empty() : oppositeRequest.getData().stream().map(MessageData::getMessageId)
        ).collect(toUnmodifiableMap(
                this::createStreamKeyFrom,
                Function.identity(),
                LATEST_SEQUENCE
        ));
    }

    private Map.Entry<@NotNull Integer, @NotNull Map<StreamKey, InnerMessageId>> processServiceResponse(
            List<MessageID> responseIds,
            SearchResult<MessageData> messages
    ) {
        if (responseIds.isEmpty()) {
            return null;
        }
        Map<StreamKey, MessageID> checkpointByDirection = associateWithStreamKey(responseIds.stream(), LATEST_SEQUENCE);

        int messageCount = 0;
        var skipAliases = new HashSet<String>(responseIds.size());
        for (MessageData data : messages.getData()) {
            MessageID messageId = data.getMessageId();
            MessageID checkpointId = checkpointByDirection.get(createStreamKeyFrom(messageId));
            String sessionAlias = messageId.getConnectionId().getSessionAlias();
            if (skipAliases.contains(sessionAlias)) {
                continue;
            }
            messageCount++;
            if (checkpointId.equals(messageId)) {
                skipAliases.add(sessionAlias);
            }
        }

        return Map.entry(messageCount, toInnerMessageIDs(checkpointByDirection));
    }

    @NotNull
    private Map<StreamKey, MessageID> associateWithStreamKey(java.util.stream.Stream<MessageID> stream, BinaryOperator<MessageID> mergeFunction) {
        return stream.collect(toMap(
                        this::createStreamKeyFrom,
                        Function.identity(),
                        mergeFunction
                ));
    }

    @NotNull
    private Map<StreamKey, InnerMessageId> toInnerMessageIDs(Map<StreamKey, MessageID> checkpointByDirection) {
        return checkpointByDirection.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        it -> fromMessageInfo(it.getValue())
                ));
    }

    @NotNull
    private StreamKey createStreamKeyFrom(MessageID messageID) {
        return new StreamKey(messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
    }

    private static InnerMessageId fromMessageInfo(MessageID messageID) {
        return new InnerMessageId(
                null,
                messageID.getSequence()
        );
    }

    private SendingReport handshake(CrawlerId crawlerId, Interval interval, DataProcessorInfo dataProcessorInfo, long numberOfEvents, long numberOfMessages) {
        DataProcessorInfo info = dataProcessor.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());

        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        if (dataProcessorName.equals(dataProcessorInfo.getName()) && dataProcessorVersion.equals(dataProcessorInfo.getVersion())) {
            LOGGER.info("Got the same name ({}) and version ({}) from repeated crawlerConnect", dataProcessorName, dataProcessorVersion);
            return new SendingReport(CrawlerAction.CONTINUE, interval, dataProcessorName, dataProcessorVersion, numberOfEvents, numberOfMessages);
        } else {
            LOGGER.info("Got another name ({}) or version ({}) from repeated crawlerConnect, restarting component", dataProcessorName, dataProcessorVersion);
            return new SendingReport(CrawlerAction.STOP, interval, dataProcessorName, dataProcessorVersion, numberOfEvents, numberOfMessages);
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
        } else {
            return createAndStoreInterval(from, from.plus(length), name, version, type, lagNow);
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

    private static class SendingReport {
        private final CrawlerAction action;
        private final String newName;
        private final String newVersion;
        private final long numberOfEvents;
        private final long numberOfMessages;
        private final Interval interval;


        private SendingReport(CrawlerAction action, Interval interval, String newName, String newVersion, long numberOfEvents, long numberOfMessages) {
            this.action = action;
            this.interval = interval;
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
        private final Map<StreamKey, MessageID> startIds;
        private final Collection<String> aliases;
        private final Instant from;
        private final Instant to;

        private MessagesInfo(Interval interval, DataProcessorInfo dataProcessorInfo,
                             Map<StreamKey, MessageID> startIds, Collection<String> aliases,
                             Instant from, Instant to) {
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
