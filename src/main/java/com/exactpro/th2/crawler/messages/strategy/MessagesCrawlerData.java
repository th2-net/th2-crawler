/*
 *  Copyright 2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.messages.strategy;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData;
import com.exactpro.th2.crawler.Continuation;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.MessagePart;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem;
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;

public class MessagesCrawlerData extends AbstractCrawlerData<MessageSearchResponse, ResumeMessageIDs, MessagePart, MessageGroupResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagesCrawlerData.class);
    private final Map<StreamKey, MessageID> startIDs;
    private final Predicate<Message> acceptMessages;
    private ResumeMessageIDs resumeMessageIDs;

    public MessagesCrawlerData(Iterator<MessageSearchResponse> data, Map<StreamKey, MessageID> startIDs, CrawlerId id, int limit, int maxSize,
                               Predicate<Message> acceptMessages) {
        super(data, id, limit, maxSize);
        this.startIDs = requireNonNull(startIDs, "'Start ids' parameter");
        this.acceptMessages = requireNonNull(acceptMessages, "'Accept messages' parameter");
    }

    @Nullable
    @Override
    protected MessageGroupResponse filterValue(MessageGroupResponse messageData) {
        if (messageData.getMessageItemCount() == 0) {
            return messageData;
        }
        List<MessageGroupItem> accepted = messageData.getMessageItemList().stream()
                .filter(it -> acceptMessages.test(it.getMessage()))
                .collect(Collectors.toList());
        if (accepted.isEmpty()) {
            return null;
        }
        if (accepted.size() == messageData.getMessageItemCount()) {
            return messageData;
        }
        return messageData.toBuilder().clearMessageItem().addAllMessageItem(accepted).build();
    }

    @Override
    protected String extractId(MessageGroupResponse last) {
        return MessageUtils.toJson(last.getMessageId());
    }

    @Override
    protected void updateState(MessageSearchResponse response) {
        if (response.hasMessageStreamPointers()) {
            Map<StreamKey, MessageID> resumeMessageIDs = MessagesStrategy.collectResumeIDs(response.getMessageStreamPointers());
            Map<StreamKey, MessageID> resumeIds = new HashMap<>(startIDs);
            MessagesStrategy.putAndCheck(resumeMessageIDs, resumeIds, "collect next resume IDs from crawler data", LOGGER);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("New resume ids: {}", resumeIds.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + MessageUtils.toJson(entry.getValue()))
                        .collect(Collectors.joining(",")));
            }
            this.resumeMessageIDs = new ResumeMessageIDs(startIDs, resumeIds);
        }
    }

    @Override
    protected @Nullable MessageGroupResponse extractValue(MessageSearchResponse response) {
        if (response.hasMessage()) {
            return response.getMessage();
        }
        return null;
    }

    @Override
    protected int extractCount(MessageGroupResponse value) {
        return value.getMessageItemCount();
    }

    @Override
    protected MessagePart buildDataPart(CrawlerId crawlerId, Collection<MessageGroupResponse> messageData) {
        return new MessagePart(crawlerId, messageData, startIDs);
    }

    @Override
    @NotNull
    public ResumeMessageIDs getContinuationInternal() {
        return requireNonNull(resumeMessageIDs, "stream info was not received");
    }

    public static class MessagePart implements SizableDataPart<MessageGroupResponse> {
        private final MessageDataRequest.Builder builder;
        private final Map<StreamKey, MessageID> startIDs;
        private MessageDataRequest request;

        private MessagePart(CrawlerId id, Collection<MessageGroupResponse> data, Map<StreamKey, MessageID> startIDs) {
            this.startIDs = requireNonNull(startIDs, "'Start ids' parameter");
            builder = MessageDataRequest.newBuilder()
                    .setId(id)
                    .addAllMessageData(data);
            request = builder.build();
        }

        public MessageDataRequest getRequest() {
            return requireNonNull(request, "request must be initialized");
        }

        public Map<StreamKey, MessageID> getStartIDs() {
            return startIDs;
        }

        @Override
        public int serializedSize() {
            return requireNonNull(request, "request must be initialized").getSerializedSize();
        }

        @Override
        public @Nullable MessageGroupResponse pullLast() {
            List<MessageGroupResponse> dataList = builder.getMessageDataList();
            if (dataList.isEmpty()) {
                return null;
            }
            int lastIndex = dataList.size() - 1;
            MessageGroupResponse last = dataList.get(lastIndex);
            builder.removeMessageData(lastIndex);
            request = builder.build();
            return last;
        }

        @Override
        public int getSize() {
            return builder.getMessageDataCount();
        }
    }

    public static class ResumeMessageIDs implements Continuation {
        private final Map<StreamKey, MessageID> startIDs;
        private final Map<StreamKey, MessageID> ids;

        public ResumeMessageIDs(Map<StreamKey, MessageID> startIDs,
                                Map<StreamKey, MessageID> ids) {
            this.startIDs = requireNonNull(startIDs, "'Start IDs' parameter");
            this.ids = requireNonNull(ids, "'Ids' parameter");
        }

        public Map<StreamKey, MessageID> getStartIDs() {
            return startIDs;
        }

        public Map<StreamKey, MessageID> getIds() {
            return ids;
        }
    }
}
