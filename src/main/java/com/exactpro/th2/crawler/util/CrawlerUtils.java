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

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProviderMethod;
import com.exactpro.th2.dataprovider.lw.grpc.BookId;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest.Builder;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest.Group;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation;
import com.google.common.collect.Iterators;
import com.google.protobuf.BoolValue;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.exactpro.th2.crawler.util.CrawlerUtilKt.maxOrDefault;
import static java.util.Objects.requireNonNullElse;

public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);
    private static final BoolValue GROUP_SORT = BoolValue.newBuilder().setValue(true).build();
    public static final Interval EMPTY = Interval.builder()
            .setBookId(new com.exactpro.cradle.BookId("Empty"))
            .setCrawlerName("Empty")
            .setCrawlerType("Empty")
            .setLastUpdate(Instant.EPOCH)
            .setStart(Instant.EPOCH)
            .setEnd(Instant.EPOCH)
            .build();

    public static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Iterator<EventSearchResponse> searchEvents(
            DataProviderService dataProviderService,
            EventsSearchParameters info,
            CrawlerMetrics metrics) {

        EventSearchRequest.Builder eventSearchBuilder = EventSearchRequest.newBuilder()
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to);
        eventSearchBuilder.getBookIdBuilder().setName(info.book);
        eventSearchBuilder.getScopeBuilder().setName(info.scopes.iterator().next()); // FIXME: pass several scopes
        LOGGER.warn("Only first {} scope is used instead of the whole {} set", info.scopes.iterator().next(), info.scopes);

        EventSearchRequest request;
        if (info.resumeId == null) {
            request = eventSearchBuilder.build();
        } else {
            LOGGER.warn("Resume id unsupported {}", info.resumeId);
            request = eventSearchBuilder.build(); // FIXME: migrate from resume id to another stream pointer
//            request = eventSearchBuilder.setResumeFromId(info.resumeId).build(); //FIXME:
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requesting events from data provider with parameters: {}", MessageUtils.toJson(request));
        }

        metrics.providerMethodInvoked(ProviderMethod.SEARCH_EVENTS);
        return collectEvents(dataProviderService.searchEvents(request), info.to);
    }

    public static Iterator<MessageSearchResponse> searchByGroups(
            DataProviderService dataProviderService,
            MessagesSearchParameters info,
            CrawlerMetrics metrics
    ) {
        Builder request = MessageGroupsSearchRequest.newBuilder()
                .setStartTimestamp(maxOrDefault(info.getResumeIds().values().stream().map(MessageID::getTimestamp), info.getFrom()))
                .setEndTimestamp(info.getTo())
                .setBookId(BookId.newBuilder().setName(info.getBook()).build())
                .addAllMessageGroup(Objects.requireNonNull(info.getStreamIds()).stream()
                        .map(it -> Group.newBuilder().setName(it).build())
                        .collect(Collectors.toUnmodifiableList()))
                .setSort(GROUP_SORT);
        metrics.providerMethodInvoked(ProviderMethod.SEARCH_MESSAGES);
        return collectMessages(dataProviderService.searchMessageGroups(request.build()), info.getTo());
    }

    @NotNull
    public static Iterator<MessageSearchResponse> searchByAliases(
            DataProviderService dataProviderService,
            MessagesSearchParameters info,
            CrawlerMetrics metrics
    ) {
        MessageSearchRequest.Builder messageSearchBuilder = MessageSearchRequest.newBuilder()
                .setStartTimestamp(info.getFrom())
                .setBookId(BookId.newBuilder().setName(info.getBook()).build());
        if (info.getTo() != null) {
            messageSearchBuilder.setEndTimestamp(info.getTo());
        }

        messageSearchBuilder.setSearchDirection(requireNonNullElse(info.getTimeRelation(), TimeRelation.NEXT));
        if (!info.getStreamIds().isEmpty()) {
            var builder = MessageStream.newBuilder();
            for (String alias : info.getStreamIds()) {
                builder.setName(alias);
                messageSearchBuilder.addStream(builder.setDirection(Direction.FIRST));
                messageSearchBuilder.addStream(builder.setDirection(Direction.SECOND));
            }
        }

        MessageSearchRequest request;
        if (info.getResumeIds().isEmpty()) {
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

    public static Iterator<EventSearchResponse> collectEvents(Iterator<EventSearchResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    public static Iterator<MessageSearchResponse> collectMessages(Iterator<MessageSearchResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasMessageId() ? messageData.getMessageId().getTimestamp() : null);
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
        private final String book;
        private final Collection<String> scopes;
        private final EventID resumeId;

        public EventsSearchParameters(Timestamp from, Timestamp to, String book, Collection<String> scopes, EventID resumeId) {
            this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
            this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
            this.book = book;
            this.scopes = scopes;
            this.resumeId = resumeId;
        }
    }

}
