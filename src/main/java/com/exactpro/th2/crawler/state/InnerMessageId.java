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

package com.exactpro.th2.crawler.state;

import java.time.Instant;
import java.util.Objects;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.th2.common.grpc.Direction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InnerMessageId {
    private final String sessionAlias;
    private final Instant timestamp;
    private final Direction direction;
    private final long sequence;

    public InnerMessageId(StoredMessage message) {
        this.sessionAlias = message.getId().toString();
        this.timestamp = message.getTimestamp();
        this.direction = Direction.valueOf(message.getDirection().name());
        this.sequence = message.getIndex();
    }

    @JsonCreator
    public InnerMessageId(
            @JsonProperty("sessionAlias") String sessionAlias,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("direction") Direction direction,
            @JsonProperty("sequence") long sequence
    ) {
        this.sessionAlias = sessionAlias;
        this.timestamp = timestamp;
        this.direction = direction;
        this.sequence = sequence;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Direction getDirection() {
        return direction;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "InnerMessage{" + CompressionUtils.EOL
                + "sessionAlias=" + sessionAlias + CompressionUtils.EOL
                + "timestamp=" + timestamp + CompressionUtils.EOL
                + "direction=" + direction + CompressionUtils.EOL
                + "sequence=" + sequence + CompressionUtils.EOL
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerMessageId that = (InnerMessageId) o;
        return sequence == that.sequence && sessionAlias.equals(that.sessionAlias) && Objects.equals(timestamp, that.timestamp) && direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionAlias, timestamp, direction, sequence);
    }
}
