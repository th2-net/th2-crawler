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

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData;
import com.exactpro.th2.crawler.Continuation;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.EventPart;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.ResumeEventId;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;

public class EventsCrawlerData extends AbstractCrawlerData<ResumeEventId, EventPart, EventData> {
    private EventData lastEvent;

    public EventsCrawlerData(Iterator<StreamResponse> data, CrawlerId id, int limit, int maxSize) {
        super(data, id, limit, maxSize);
    }

    @Nullable
    @Override
    public ResumeEventId getContinuationInternal() {
        EventData data = lastEvent;
        return data == null ? null : resumeIdFromEvent(data);
    }

    @Override
    protected String extractId(EventData last) {
        return MessageUtils.toJson(last.getEventId());
    }

    @Override
    protected void updateState(StreamResponse response) {
        if (response.hasEvent()) {
            lastEvent = response.getEvent();
        }
    }

    @Override
    protected @Nullable EventData extractValue(StreamResponse response) {
        if (response.hasEvent()) {
            return response.getEvent();
        }
        return null;
    }

    @Override
    protected EventPart buildDataPart(CrawlerId crawlerId, Collection<EventData> eventData) {
        return new EventPart(crawlerId, eventData);
    }

    public static ResumeEventId resumeIdFromEvent(EventData data) {
        return new ResumeEventId(data.getEventId(), CrawlerUtils.fromTimestamp(data.getStartTimestamp()));
    }

    public static class EventPart implements SizableDataPart<EventData> {
        private final EventDataRequest.Builder builder;
        private EventDataRequest request;

        private EventPart(CrawlerId id, Collection<EventData> data) {
            this.builder = EventDataRequest.newBuilder()
                    .setId(id)
                    .addAllEventData(data);
            request = builder.build();
        }

        public EventDataRequest getRequest() {
            return requireNonNull(request, "request must be initialized");
        }

        @Override
        public int serializedSize() {
            return requireNonNull(request, "request must be initialized").getSerializedSize();
        }

        @Override
        public @Nullable EventData pullLast() {
            List<EventData> dataList = builder.getEventDataList();
            if (dataList.isEmpty()) {
                return null;
            }
            int lastIndex = dataList.size() - 1;
            EventData last = dataList.get(lastIndex);
            builder.removeEventData(lastIndex);
            request = builder.build();
            return last;
        }

        @Override
        public int getSize() {
            return builder.getEventDataCount();
        }
    }

    public static class ResumeEventId implements Continuation {
        private final EventID resumeId;
        private final Instant timestamp;

        public ResumeEventId(EventID resumeId, Instant timestamp) {
            this.resumeId = requireNonNull(resumeId, "'Resume id' parameter");
            this.timestamp = requireNonNull(timestamp, "'Timestamp' parameter");
        }

        public EventID getResumeId() {
            return resumeId;
        }

        public Instant getTimestamp() {
            return timestamp;
        }
    }
}
