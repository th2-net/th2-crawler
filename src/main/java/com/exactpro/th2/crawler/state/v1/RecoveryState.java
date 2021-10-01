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

package com.exactpro.th2.crawler.state.v1;

import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.th2.crawler.state.BaseState;
import com.exactpro.th2.crawler.state.Version;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class RecoveryState implements BaseState {
    private final InnerEventId lastProcessedEvent;

    @JsonDeserialize(converter = StreamKeyMapConverter.Deserialize.class)
    @JsonSerialize(converter = StreamKeyMapConverter.Serialize.class)
    private final Map<StreamKey, InnerMessageId> lastProcessedMessages;

    private final long lastNumberOfEvents;

    private final long lastNumberOfMessages;

    @JsonCreator
    public RecoveryState(@JsonProperty("lastProcessedEvent") InnerEventId lastProcessedEvent,
                         @JsonProperty("lastProcessedMessages") Map<StreamKey, InnerMessageId> lastProcessedMessages,
                         @JsonProperty("lastNumberOfEvents") long lastNumberOfEvents,
                         @JsonProperty("lastNumberOfMessages") long lastNumberOfMessages) {
        this.lastProcessedEvent = lastProcessedEvent;
        this.lastProcessedMessages = lastProcessedMessages;
        this.lastNumberOfEvents = lastNumberOfEvents;
        this.lastNumberOfMessages = lastNumberOfMessages;
    }

    public InnerEventId getLastProcessedEvent() { return lastProcessedEvent; }

    public Map<StreamKey, InnerMessageId> getLastProcessedMessages() { return lastProcessedMessages; }

    public long getLastNumberOfEvents() { return lastNumberOfEvents; }

    public long getLastNumberOfMessages() { return lastNumberOfMessages; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("RecoveryState{").append(CompressionUtils.EOL)
                .append("lastProcessedEvent=")
                .append(lastProcessedEvent).append(CompressionUtils.EOL)
                .append("lastProcessedMessages=");

        if (lastProcessedMessages != null) {
            StringJoiner joiner = new StringJoiner(", ");
            lastProcessedMessages.forEach((k, v) -> joiner.add(k + ": "+ v));
            sb.append(joiner);
        } else {
            sb.append("null").append(CompressionUtils.EOL);
        }

        sb.append("lastNumberOfEvents=")
                .append(lastNumberOfEvents).append(CompressionUtils.EOL)
                .append("lastNumberOfMessages=")
                .append(lastNumberOfMessages).append(CompressionUtils.EOL)
                .append("}");

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecoveryState that = (RecoveryState) o;
        return lastNumberOfEvents == that.lastNumberOfEvents && lastNumberOfMessages == that.lastNumberOfMessages && Objects.equals(lastProcessedEvent, that.lastProcessedEvent) && Objects.equals(lastProcessedMessages, that.lastProcessedMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastProcessedEvent, lastProcessedMessages, lastNumberOfEvents, lastNumberOfMessages);
    }
}
