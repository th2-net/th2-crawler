/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.state.v2.StreamKey;
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation;
import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class MessagesSearchParameters {
    private final Timestamp from;
    private final Timestamp to;
    private final Map<StreamKey, MessageID> resumeIds;
    private final Collection<String> streamIds;
    private final String book;
    private final TimeRelation timeRelation = TimeRelation.NEXT;

    private MessagesSearchParameters(
            Timestamp from,
            Timestamp to,
            String book,
            Collection<String> streamIds,
            Map<StreamKey, MessageID> resumeIds
    ) {
        if ((streamIds == null || streamIds.isEmpty()) && (resumeIds == null || resumeIds.isEmpty())) {
            throw new IllegalArgumentException("either streamIds or resumeIds must be set");
        }
        if (StringUtils.isBlank(book)) { throw new IllegalArgumentException("'book' can't be blank"); }
        this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
        this.to = to;
        this.book = book;
        this.resumeIds = resumeIds;
        this.streamIds = streamIds;
    }

    public Timestamp getFrom() {
        return from;
    }

    @Nullable
    public Timestamp getTo() {
        return to;
    }

    @Nullable
    public Map<StreamKey, MessageID> getResumeIds() {
        return resumeIds;
    }

    public String getBook() {
        return book;
    }

    public Collection<String> getStreamIds() {
        return streamIds;
    }

    public TimeRelation getTimeRelation() {
        return timeRelation;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Timestamp from;
        private Timestamp to;
        private Map<StreamKey, MessageID> resumeIds;
        private Collection<String> streamIds;
        private String book;

        private Builder() {
        }

        public Builder setFrom(Timestamp from) {
            this.from = from;
            return this;
        }

        public Builder setTo(Timestamp to) {
            this.to = to;
            return this;
        }

        public Builder setResumeIds(Map<StreamKey, MessageID> resumeIds) {
            this.resumeIds = resumeIds;
            return this;
        }

        public Builder setStreamIds(Collection<String> streamIds) {
            this.streamIds = streamIds;
            return this;
        }

        public Builder setBook(String book) {
            this.book = book;
            return this;
        }

        public MessagesSearchParameters build() {
            return new MessagesSearchParameters(from, to, book, streamIds, resumeIds);
        }
    }
}
