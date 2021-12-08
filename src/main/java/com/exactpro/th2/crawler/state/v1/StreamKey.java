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

import java.util.Objects;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamKey {
    private final String sessionAlias;
    private final Direction direction;

    public StreamKey(
            @JsonProperty("sessionAlias") String sessionAlias,
            @JsonProperty("direction") Direction direction
    ) {
        this.sessionAlias = sessionAlias;
        this.direction = direction;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamKey streamKey = (StreamKey) o;
        return sessionAlias.equals(streamKey.sessionAlias) && direction == streamKey.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionAlias, direction);
    }

    @Override
    public String toString() {
        return sessionAlias + " (" + direction + ")";
    }

    public static StreamKey fromMessageId(MessageID id) {
        return new StreamKey(id.getConnectionId().getSessionAlias(), id.getDirection());
    }
}
