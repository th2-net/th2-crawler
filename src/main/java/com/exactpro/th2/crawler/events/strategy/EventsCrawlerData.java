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

package com.exactpro.th2.crawler.events.strategy;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData;
import com.exactpro.th2.crawler.Continuation;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.ResumeEventId;
import com.exactpro.th2.dataprovider.grpc.EventData;

public class EventsCrawlerData extends AbstractCrawlerData<ResumeEventId, EventData> {
    private final ResumeEventId resumeEventId;

    public EventsCrawlerData(List<EventData> data, ResumeEventId resumeEventId, boolean needsNextRequest) {
        super(data, needsNextRequest);
        this.resumeEventId = resumeEventId;
    }

    @Nullable
    @Override
    public ResumeEventId getContinuation() {
        return resumeEventId;
    }

    public static class ResumeEventId implements Continuation {
        private final EventID resumeId;
        private final Instant timestamp;

        public ResumeEventId(EventID resumeId, Instant timestamp) {
            this.resumeId = Objects.requireNonNull(resumeId, "'Resume id' parameter");
            this.timestamp = Objects.requireNonNull(timestamp, "'Timestamp' parameter");
        }

        public EventID getResumeId() {
            return resumeId;
        }

        public Instant getTimestamp() {
            return timestamp;
        }
    }
}
