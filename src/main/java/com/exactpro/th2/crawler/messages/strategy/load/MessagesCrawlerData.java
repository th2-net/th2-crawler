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

package com.exactpro.th2.crawler.messages.strategy.load;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData;
import com.exactpro.th2.crawler.Continuation;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.CrawlerData;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.messages.strategy.load.MessagesCrawlerData.MessagePart;
import com.exactpro.th2.crawler.messages.strategy.load.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsResponse;
import com.exactpro.th2.dataprovider.grpc.MessageIntervalInfo;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class MessagesCrawlerData implements CrawlerData<ResumeMessageIDs, MessagePart> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagesCrawlerData.class);
    private final Map<StreamKey, MessageID> startIDs;
    private final Predicate<Message> acceptMessages;
    private ResumeMessageIDs resumeMessageIDs;

    public MessagesCrawlerData(CrawlerMetrics metrics,
                               CrawlerConfiguration debug,
                               CradleMessageGroupsResponse data,
                               Map<StreamKey, MessageID> startIDs,
                               CrawlerId id,
                               Predicate<Message> acceptMessages) {
        this.startIDs = requireNonNull(startIDs, "'Start ids' parameter");
        this.acceptMessages = requireNonNull(acceptMessages, "'Accept messages' parameter");
    }

    @Override
    public boolean getHasData() {
        return true;
    }

    @Nullable
    @Override
    public ResumeMessageIDs getContinuation() {
        return null;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public MessagePart next() {
        return null;
    }

    public static class MessagePart implements AbstractCrawlerData.SizableDataPart<MessageIntervalInfo> {
        private final MessageIntervalInfo info;

        private MessagePart(MessageIntervalInfo info) {
            this.info = info;
        }

        @Override
        public int serializedSize() {
            throw new UnsupportedOperationException("The class " + getClass().getName() + " doesn't support the serializedSize operation");
        }

        @Override
        public @Nullable MessageIntervalInfo pullLast() {
            return info;
        }

        @Override
        public int getSize() {
            throw new UnsupportedOperationException("The class " + getClass().getName() + " doesn't support the getSize operation");
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
