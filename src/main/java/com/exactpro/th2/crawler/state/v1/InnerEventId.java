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

import com.exactpro.cradle.utils.CompressionUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InnerEventId {
    private final Instant startTimestamp;
    private final String id;

    @JsonCreator
    public InnerEventId(
            @JsonProperty("startTimestamp") Instant startTimestamp,
            @JsonProperty("id") String id
    ) {
        this.startTimestamp = startTimestamp;
        this.id = id;
    }

    public Instant getStartTimestamp() {
        return startTimestamp;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "InnerEvent{" + CompressionUtils.EOL
                + "id=" + id + CompressionUtils.EOL
                + "startTimestamp=" + startTimestamp + CompressionUtils.EOL
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerEventId that = (InnerEventId) o;
        return startTimestamp.equals(that.startTimestamp) && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimestamp, id);
    }
}
