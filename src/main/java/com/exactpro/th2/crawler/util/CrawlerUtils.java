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

import static java.util.Objects.requireNonNullElse;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.Method;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProviderMethod;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StreamsInfo;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
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

    public static SearchResult<EventData> searchEvents(DataProviderService dataProviderService,
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
        return metrics.measureTime(DataType.EVENTS, Method.REQUEST_DATA, () -> collectEvents(dataProviderService.searchEvents(request), info.to));
    }

    public static SearchResult<MessageData> searchMessages(DataProviderService dataProviderService,
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
        return metrics.measureTime(DataType.MESSAGES, Method.REQUEST_DATA, () -> collectMessages(dataProviderService.searchMessages(request), info.getTo()));
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

    public static SearchResult<EventData> collectEvents(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    public static SearchResult<MessageData> collectMessages(Iterator<StreamResponse> iterator, Timestamp to) {
        SearchResult<MessageData> result = collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasTimestamp() ? messageData.getTimestamp() : null);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Data from provider " + CrawlerUtilKt.toCompactString(result));
        }
        return result;
    }

    public static <T extends MessageOrBuilder> SearchResult<T> collectData(Iterator<StreamResponse> iterator, Timestamp to,
                                                                   Function<StreamResponse, T> objectExtractor,
                                                                   Function<T, Timestamp> timeExtractor) {
        List<T> data = null;
        StreamsInfo streamsInfo = null;

        LOGGER.trace("Iterator traversal is started");
        while (iterator.hasNext()) {
            StreamResponse r = iterator.next();
            if (r.hasStreamInfo()) {
                streamsInfo = r.getStreamInfo();
                continue;
            }

            T object = objectExtractor.apply(r);
            if (object == null) {
                continue;
            }
            if (data == null) {
                data = new ArrayList<>();
            }

            if (to != null && !to.equals(timeExtractor.apply(object))) {
                data.add(object);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Got object of type {}: {}", object.getClass().getSimpleName(), MessageUtils.toJson(object));
                }
            }
        }
        LOGGER.trace("Iterator traversal is ended");

        return new SearchResult<>(data == null ? Collections.emptyList() : data, streamsInfo);
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
