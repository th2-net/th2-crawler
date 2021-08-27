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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.protobuf.Timestamp;

public class MessagesSearchParameters {
    private final Timestamp from;
    private final Timestamp to;
    private int batchSize = 1;
    private Map<StreamKey, MessageID> resumeIds;
    private Collection<String> aliases;
    private TimeRelation timeRelation = TimeRelation.NEXT;

    private MessagesSearchParameters(
            Timestamp from,
            Timestamp to
    ) {
        this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
        this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
    }

    public Timestamp getFrom() {
        return from;
    }

    public Timestamp getTo() {
        return to;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Map<StreamKey, MessageID> getResumeIds() {
        return resumeIds;
    }

    public Collection<String> getAliases() {
        return aliases;
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
        private int batchSize = 1;
        private Map<StreamKey, MessageID> resumeIds;
        private Collection<String> aliases;
        private TimeRelation timeRelation = TimeRelation.NEXT;
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

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setResumeIds(Map<StreamKey, MessageID> resumeIds) {
            this.resumeIds = resumeIds;
            return this;
        }

        public Builder setAliases(Collection<String> aliases) {
            this.aliases = aliases;
            return this;
        }

        public Builder setTimeRelation(TimeRelation timeRelation) {
            this.timeRelation = timeRelation;
            return this;
        }

        public MessagesSearchParameters build() {
            if (aliases == null && resumeIds == null) {
                throw new IllegalArgumentException("either aliases or resumeIds must be set");
            }
            MessagesSearchParameters parameters = new MessagesSearchParameters(from, to);
            parameters.batchSize = batchSize;
            parameters.aliases = aliases;
            parameters.resumeIds = resumeIds;
            parameters.timeRelation = timeRelation;
            return parameters;
        }
    }
}
