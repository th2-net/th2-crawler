/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.util;

import static com.exactpro.th2.common.grpc.Direction.FIRST;
import static com.exactpro.th2.common.grpc.Direction.SECOND;
import static java.util.Objects.requireNonNullElse;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProviderMethod;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.common.collect.Iterators;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;

public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);
    private static final BoolValue METADATA_ONLY = BoolValue.newBuilder().setValue(false).build();
    public static final Interval EMPTY = Interval.builder()
            .crawlerName("Empty")
            .crawlerType("Empty")
            .startTime(Instant.EPOCH)
            .endTime(Instant.EPOCH)
            .build();

    public static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Iterator<StreamResponse> searchEvents(DataProviderService dataProviderService,
                                                        EventsSearchParameters info, CrawlerMetrics metrics) {

        EventSearchRequest.Builder eventSearchBuilder = EventSearchRequest.newBuilder()
                .setMetadataOnly(METADATA_ONLY)
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to)
                .setResultCountLimit(Int32Value.of(info.batchSize));

        EventSearchRequest request;
        if (info.resumeId == null) {
            request = eventSearchBuilder.build();
        } else {
            request = eventSearchBuilder.setResumeFromId(info.resumeId).build();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requesting events from data provider with parameters: {}", MessageUtils.toJson(request));
        }

        metrics.providerMethodInvoked(ProviderMethod.SEARCH_EVENTS);
        return collectEvents(dataProviderService.searchEvents(request), info.to);
    }

    public static Iterator<StreamResponse> searchMessages(DataProviderService dataProviderService,
                                                   MessagesSearchParameters info, CrawlerMetrics metrics) {

        MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
                .setStartTimestamp(info.getFrom());
        if (info.getTo() != null) {
            messageSearchBuilder.setEndTimestamp(info.getTo());
        }
        messageSearchBuilder.setResultCountLimit(Int32Value.of(info.getBatchSize()))
                .setSearchDirection(requireNonNullElse(info.getTimeRelation(), TimeRelation.NEXT));
        if (info.getAliases() != null) {
            messageSearchBuilder.setStream(StringList.newBuilder().addAllListString(info.getAliases()).build());
        }


        MessageSearchRequest request;
        if (info.getResumeIds() == null || info.getResumeIds().isEmpty()) {
            request = messageSearchBuilder.build();
        } else {
            request = messageSearchBuilder.addAllMessageId(info.getResumeIds().values()).build();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requesting messages from data provider with parameters: {}", MessageUtils.toJson(request));
        }
        metrics.providerMethodInvoked(ProviderMethod.SEARCH_MESSAGES);

        List<StreamResponse> list = new ArrayList<>();

        if (LOGGER.isDebugEnabled()) {
            dataProviderService.searchMessages(request).forEachRemaining(list::add);

            list.stream().filter(response -> !response.getStreamInfo().toString().isBlank())
                    .forEach(response -> LOGGER.debug("StreamInfo: {}", response.getStreamInfo()));
        }

        return collectMessages(list.isEmpty() ? dataProviderService.searchMessages(request) : list.iterator(), info.getTo());
    }

    public static void updateEventRecoveryState(
            IntervalsWorker worker,
            InternalInterval interval,
            long numberOfEvents
    ) throws IOException {
        RecoveryState currentState = interval.getState();
        InnerEventId lastProcessedEvent = null;

        if (currentState != null) {
            lastProcessedEvent = currentState.getLastProcessedEvent();
        }


        RecoveryState newState;
        if (currentState == null) {
            newState = new RecoveryState(
                    null,
                    null,
                    numberOfEvents,
                    0);
        } else {
            newState = new RecoveryState(
                    lastProcessedEvent,
                    currentState.getLastProcessedMessages(),
                    numberOfEvents,
                    currentState.getLastNumberOfMessages());
        }

        interval.updateState(newState, worker);
    }

    public static Interval updateMessageRecoveryState(
            IntervalsWorker worker,
            Interval interval,
            StateService<RecoveryState> stateService,
            long numberOfMessages
    ) throws IOException {
        RecoveryState currentState = interval.getRecoveryState() == null
                ? null
                : stateService.deserialize(interval.getRecoveryState());
        Map<StreamKey, InnerMessageId> lastProcessedMessages = new HashMap<>();

        if (currentState != null && currentState.getLastProcessedMessages() != null) {
            lastProcessedMessages.putAll(currentState.getLastProcessedMessages());
        }

        RecoveryState newState;
        if (currentState == null) {
            newState = new RecoveryState(
                    null,
                    lastProcessedMessages,
                    0,
                    numberOfMessages
            );
        } else {
            newState = new RecoveryState(
                    currentState.getLastProcessedEvent(),
                    lastProcessedMessages,
                    currentState.getLastNumberOfEvents(),
                    numberOfMessages);
        }

        return worker.updateRecoveryState(interval, stateService.serialize(newState));
    }

    public static Iterator<StreamResponse> collectEvents(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    public static Iterator<StreamResponse> collectMessages(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasTimestamp() ? messageData.getTimestamp() : null);
    }

    public static void fillOppositeStream(Map<StreamKey, MessageID> ids) {
        var entries = ids.entrySet();
        var keys = ids.keySet();

        entries.iterator().forEachRemaining(entry -> {
            StreamKey key = entry.getKey();
            Direction oppositeDirection = getOppositeDirection(key.getDirection());
            StreamKey streamKey = new StreamKey(key.getSessionAlias(), oppositeDirection);

            if (!keys.contains(streamKey)) {

                LOGGER.debug("Create message id for stream key {} as it was absent", streamKey);

                MessageID id = MessageID.newBuilder()
                        .setConnectionId(ConnectionID.newBuilder().setSessionAlias(key.getSessionAlias()).build())
                        .setDirection(oppositeDirection)
                        .setSequence(-1)
                        .build();
                ids.put(streamKey, id);
            }
        });
    }

    public static Direction getOppositeDirection(Direction direction) {
        return direction == FIRST ? SECOND : FIRST;
    }

    public static <T extends MessageOrBuilder> Iterator<StreamResponse> collectData(Iterator<StreamResponse> iterator, Timestamp to,
                                                                   Function<StreamResponse, T> objectExtractor,
                                                                   Function<T, Timestamp> timeExtractor) {
        return Iterators.filter(iterator, el -> {
            T obj = objectExtractor.apply(el);
            return obj == null || !to.equals(timeExtractor.apply(obj));
        });
    }

    public static class EventsSearchParameters {
        private final Timestamp from;
        private final Timestamp to;
        private final int batchSize;
        private final EventID resumeId;

        public EventsSearchParameters(Timestamp from, Timestamp to,
                                      int batchSize, EventID resumeId) {
            this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
            this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
            this.batchSize = batchSize;
            this.resumeId = resumeId;
        }
    }

}
