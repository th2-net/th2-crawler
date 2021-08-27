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

package com.exactpro.th2.crawler.state.v1;

import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

import com.exactpro.cradle.utils.CompressionUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InnerMessageId {
    private final Instant timestamp;
    private final long sequence;

    @JsonCreator
    public InnerMessageId(
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("sequence") long sequence
    ) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", InnerMessageId.class.getSimpleName() + "[", "]")
                .add("timestamp=" + timestamp)
                .add("sequence=" + sequence)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerMessageId that = (InnerMessageId) o;
        return sequence == that.sequence && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sequence);
    }
}
