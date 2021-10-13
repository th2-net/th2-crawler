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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData;
import com.exactpro.th2.crawler.Continuation;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.MessageData;

public class MessagesCrawlerData extends AbstractCrawlerData<ResumeMessageIDs, MessageData> {
    private final ResumeMessageIDs resumeMessageIDs;

    public MessagesCrawlerData(List<MessageData> data, ResumeMessageIDs resumeMessageIDs, boolean needsNextRequest) {
        super(data, needsNextRequest);
        this.resumeMessageIDs = Objects.requireNonNull(resumeMessageIDs, "'Resume message i ds' parameter");
    }

    @Override
    @NotNull
    public ResumeMessageIDs getContinuation() {
        return resumeMessageIDs;
    }

    public static class ResumeMessageIDs implements Continuation {
        private final Map<StreamKey, MessageID> startIDs;
        private final Map<StreamKey, MessageID> ids;

        public ResumeMessageIDs(Map<StreamKey, MessageID> startIDs,
                                Map<StreamKey, MessageID> ids) {
            this.startIDs = Objects.requireNonNull(startIDs, "'Start IDs' parameter");
            this.ids = Objects.requireNonNull(ids, "'Ids' parameter");
        }

        public Map<StreamKey, MessageID> getStartIDs() {
            return startIDs;
        }

        public Map<StreamKey, MessageID> getIds() {
            return ids;
        }
    }
}
