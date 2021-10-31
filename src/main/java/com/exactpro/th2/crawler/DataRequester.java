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

package com.exactpro.th2.crawler;

import static java.util.Objects.requireNonNullElse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRequester {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataRequester.class);

    private final DataProviderService dataProviderService;

    public DataRequester(DataProviderService dataProviderService) {
        this.dataProviderService = dataProviderService;
    }

    public SearchResult<EventData> searchEvents(EventsSearchParameters params) {
        EventSearchRequest request = buildEventSearchRequest(params);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Send events. Request: {}", MessageUtils.toJson(request));
        }
        return collectEvents(dataProviderService.searchEvents(request), params.getTo());
    }

    public SearchResult<MessageData> searchMessages(MessagesSearchParameters params) {
        MessageSearchRequest request = buildMessageSearchRequest(params);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Request messages from DataProviderService. Request: {}", MessageUtils.toJson(request));
        }
        Iterator<StreamResponse> streamResponseIterator = dataProviderService.searchMessages(request);
        LOGGER.debug("Got response from DataProviderService.");
        return collectMessages(streamResponseIterator, params.getTo());
    }

    private static SearchResult<EventData> collectEvents(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasEvent() ? response.getEvent() : null,
                eventData -> eventData.hasStartTimestamp() ? eventData.getStartTimestamp() : null);
    }

    private static SearchResult<MessageData> collectMessages(Iterator<StreamResponse> iterator, Timestamp to) {
        return collectData(iterator, to, response -> response.hasMessage() ? response.getMessage() : null,
                messageData -> messageData.hasTimestamp() ? messageData.getTimestamp() : null);
    }

    private static <T extends MessageOrBuilder> SearchResult<T> collectData(Iterator<StreamResponse> iterator, Timestamp to,
                                                                           Function<StreamResponse, T> objectExtractor,
                                                                           Function<T, Timestamp> timeExtractor) {
        List<T> data = null;
        StreamsInfo streamsInfo = null;
        LOGGER.debug("Collecting data...");

        int i = 0;
        while (iterator.hasNext()) {
            LOGGER.debug("Iteration {}", ++i);
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
                LOGGER.debug("Collected one more data object");

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Got object of type {}: {}", object.getClass().getSimpleName(), MessageUtils.toJson(object));
                }
            }
        }
        if (data == null) {
            LOGGER.debug("Nothing collected");
        } else {
            LOGGER.debug("Collected {} objects", data.size());
        }
        return new SearchResult<>(data == null ? Collections.emptyList() : data, streamsInfo);
    }

    private MessageSearchRequest buildMessageSearchRequest(MessagesSearchParameters info) {
        MessageSearchRequest.Builder requestBuilder = MessageSearchRequest.newBuilder()
                .setStartTimestamp(info.getFrom())
                .setResultCountLimit(Int32Value.of(info.getBatchSize()))
                .setSearchDirection(requireNonNullElse(info.getTimeRelation(), TimeRelation.NEXT))
                .setStream(StringList.newBuilder().addAllListString(info.getAliases()).build());

        if (info.getTo() != null) {
            requestBuilder.setEndTimestamp(info.getTo());
        }

        if (info.getResumeIds() != null && !info.getResumeIds().isEmpty()) {
            requestBuilder.addAllMessageId(info.getResumeIds().values()).build();
        }
        return requestBuilder.build();
    }

    private EventSearchRequest buildEventSearchRequest(EventsSearchParameters params) {
         EventSearchRequest.Builder eventSearchBuilder = EventSearchRequest.newBuilder()
                .setMetadataOnly(BoolValue.newBuilder().setValue(false).build())
                .setStartTimestamp(params.getFrom())
                .setEndTimestamp(params.getTo())
                .setResultCountLimit(Int32Value.of(params.getBatchSize()));

        if (params.getResumeId() != null) {
            eventSearchBuilder.setResumeFromId(params.getResumeId());
        }
        return eventSearchBuilder.build();
    }
}
