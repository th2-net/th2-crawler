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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.Direction;
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
import com.exactpro.th2.dataprovider.grpc.EventSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest.Builder;
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest.Group;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.common.collect.Iterators;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;

public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);
    private static final BoolValue METADATA_ONLY = BoolValue.newBuilder().setValue(false).build();

    private static final BoolValue GROUP_SORT = BoolValue.newBuilder().setValue(true).build();
    public static final Interval EMPTY = Interval.builder()
            .crawlerName("Empty")
            .crawlerType("Empty")
            .startTime(Instant.EPOCH)
            .endTime(Instant.EPOCH)
            .build();

    public static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Iterator<EventSearchResponse> searchEvents(DataProviderService dataProviderService,
                                                             EventsSearchParameters info, CrawlerMetrics metrics) {

        EventSearchRequest.Builder eventSearchBuilder = EventSearchRequest.newBuilder()
                .setMetadataOnly(METADATA_ONLY)
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to);

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

    public static Iterator<MessageSearchResponse> searchMessages(DataProviderService dataProviderService,
                                                   MessagesSearchParameters info, CrawlerMetrics metrics) {

        return info.isUseGroups()
                ? searchByGroups(dataProviderService, info, metrics)
                : searchByAliases(dataProviderService, info, metrics);
    }

    private static Iterator<MessageSearchResponse> searchByGroups(
            DataProviderService dataProviderService,
            MessagesSearchParameters info,
            CrawlerMetrics metrics
    ) {
        Builder request = MessageGroupsSearchRequest.newBuilder()
                .setStartTimestamp(info.getFrom())
                .setEndTimestamp(info.getTo())
                .addAllMessageGroup(Objects.requireNonNull(info.getAliases()).stream()
                        .map(it -> Group.newBuilder().setName(it).build())
                        .collect(Collectors.toUnmodifiableList()))
                .setKeepOpen(true)
                .setSort(GROUP_SORT);
        metrics.providerMethodInvoked(ProviderMethod.SEARCH_MESSAGES);
        return collectMessages(dataProviderService.searchMessageGroups(request.build()), info.getTo());
    }

    @NotNull
    private static Iterator<MessageSearchResponse> searchByAliases(DataProviderService dataProviderService, MessagesSearchParameters info,
            CrawlerMetrics metrics) {
        MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
                .setStartTimestamp(info.getFrom());
        if (info.getTo() != null) {
            messageSearchBuilder.setEndTimestamp(info.getTo());
        }

        Integer limit = info.getBatchSize();
        if (limit != null) {
            messageSearchBuilder.setResultCountLimit(Int32Value.of(limit));
        }
        messageSearchBuilder.setSearchDirection(requireNonNullElse(info.getTimeRelation(), TimeRelation.NEXT));
        if (info.getAliases() != null) {
            var builder = MessageStream.newBuilder();
            for (String alias : info.getAliases()) {
                builder.setName(alias);
                messageSearchBuilder.addStream(builder.setDirection(Direction.FIRST));
                messageSearchBuilder.addStream(builder.setDirection(Direction.SECOND));
            }
        }

        MessageSearchRequest request;
        if (info.getResumeIds() == null || info.getResumeIds().isEmpty()) {
            request = messageSearchBuilder.build();
        } else {
            request = messageSearchBuilder.addAllStreamPointer(info.getResumeIds().entrySet().stream()
                    .map(entry -> MessageStreamPointer.newBuilder()
                            .setLastId(entry.getValue())
                            .setMessageStream(MessageStream.newBuilder().setName(entry.getKey().getSessionAlias()).setDirection(entry.getKey().getDirection()))
                            .build())
                    .collect(Collectors.toList())).build();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requesting messages from data provider with parameters: {}", MessageUtils.toJson(request));
        }
        metrics.providerMethodInvoked(ProviderMethod.SEARCH_MESSAGES);
        return collectMessages(dataProviderService.searchMessages(request), info.getTo());
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

    public static Iterator<EventSearchResponse> collectEvents(Iterator<EventSearchResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    public static Iterator<MessageSearchResponse> collectMessages(Iterator<MessageSearchResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasTimestamp() ? messageData.getTimestamp() : null);
    }

    public static <T extends MessageOrBuilder, R extends MessageOrBuilder> Iterator<R> collectData(Iterator<R> iterator, Timestamp to,
                                                                   Function<R, T> objectExtractor,
                                                                   Function<T, Timestamp> timeExtractor) {
        return Iterators.filter(iterator, el -> {
            T obj = objectExtractor.apply(el);
            return obj == null || !to.equals(timeExtractor.apply(obj));
        });
    }

    public static class EventsSearchParameters {
        private final Timestamp from;
        private final Timestamp to;
        private final EventID resumeId;

        public EventsSearchParameters(Timestamp from, Timestamp to, EventID resumeId) {
            this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
            this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
            this.resumeId = resumeId;
        }
    }

}
