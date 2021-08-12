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

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);

    public static List<EventData> searchEvents(DataProviderService dataProviderService,
                                               EventsSearchInfo info) {

        EventSearchRequest.Builder eventSearchBuilder = info.searchBuilder;
        EventSearchRequest request;

        eventSearchBuilder
                .setMetadataOnly(BoolValue.newBuilder().setValue(false).build())
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to)
                .setResultCountLimit(Int32Value.of(info.batchSize));

        if (info.resumeId == null)
            request = eventSearchBuilder.build();
        else
            request = eventSearchBuilder.setResumeFromId(info.resumeId).build();

        Iterator<StreamResponse> iterator = dataProviderService.searchEvents(request);

        return collectEvents(iterator, info.to);
    }

    public static List<MessageData> searchMessages(DataProviderService dataProviderService,
                                                   MessagesSearchInfo info) {

        MessageSearchRequest.Builder messageSearchBuilder = info.searchBuilder;
        MessageSearchRequest request;

        messageSearchBuilder
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to)
                .setResultCountLimit(Int32Value.of(info.batchSize))
                .setStream(StringList.newBuilder().addAllListString(info.aliases).build());

        if (info.resumeIds == null)
            request = messageSearchBuilder.build();
        else
            request = messageSearchBuilder.addAllMessageId(info.resumeIds.values()).build();

        Iterator<StreamResponse> iterator = dataProviderService.searchMessages(request);

        return collectMessages(iterator, info.to);
    }

    private static List<EventData> collectEvents(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    private static List<MessageData> collectMessages(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasTimestamp() ? messageData.getTimestamp() : null);
    }

    public static <T extends MessageOrBuilder> List<T> collectData(Iterator<StreamResponse> iterator, Timestamp to,
                                                                   Function<StreamResponse, T> objectExtractor,
                                                                   Function<T, Timestamp> timeExtractor) {
        List<T> data = new ArrayList<>();

        while (iterator.hasNext()) {
            StreamResponse r = iterator.next();

            T object = objectExtractor.apply(r);
            if (object == null) {
                continue;
            }

            if (!to.equals(timeExtractor.apply(object))) {
                data.add(object);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Got {}", MessageUtils.toJson(object, true));
                }
            }
        }

        return data;
    }

    public static class EventsSearchInfo {
        private final EventSearchRequest.Builder searchBuilder;
        private final Timestamp from;
        private final Timestamp to;
        private final int batchSize;
        private final EventID resumeId;

        public EventsSearchInfo(EventSearchRequest.Builder searchBuilder, Timestamp from, Timestamp to,
                                int batchSize, EventID resumeId) {
            this.searchBuilder = Objects.requireNonNull(searchBuilder, "Search builder must not be null");
            this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
            this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
            this.batchSize = batchSize;
            this.resumeId = resumeId;
        }
    }

    public static class MessagesSearchInfo {
        private final MessageSearchRequest.Builder searchBuilder;
        private final Timestamp from;
        private final Timestamp to;
        private final int batchSize;
        private final Map<String, MessageID> resumeIds;
        private final Collection<String> aliases;

        public MessagesSearchInfo(MessageSearchRequest.Builder searchBuilder, Timestamp from, Timestamp to,
                                  int batchSize, Map<String, MessageID> resumeIds, Collection<String> aliases) {
            this.searchBuilder = Objects.requireNonNull(searchBuilder, "Search builder must not be null");
            this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
            this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
            this.batchSize = batchSize;
            this.resumeIds = resumeIds;
            this.aliases = aliases;
        }
    }
}
