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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.util.StdConverter;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class StreamKeyMapConverter {
    public static class Serialize extends StdConverter<Map<StreamKey, InnerMessageId>, List<StreamMappingAdapter>> {
        @Override
        public List<StreamMappingAdapter> convert(Map<StreamKey, InnerMessageId> value) {
            return value == null ? null : value.entrySet().stream()
                    .map(StreamMappingAdapter::new)
                    .collect(toUnmodifiableList());
        }
    }

    public static class Deserialize extends StdConverter<List<StreamMappingAdapter>, Map<StreamKey, InnerMessageId>> {
        @Override
        public Map<StreamKey, InnerMessageId> convert(List<StreamMappingAdapter> value) {
            return value == null ? null : value.stream()
                    .collect(Collectors.toUnmodifiableMap(
                            StreamMappingAdapter::getKey,
                            StreamMappingAdapter::getMessage
                    ));
        }
    }

    static class StreamMappingAdapter {
        private final StreamKey key;
        private final InnerMessageId message;

        @JsonCreator
        public StreamMappingAdapter(
                @JsonProperty("key") StreamKey key,
                @JsonProperty("message") InnerMessageId message
        ) {
            this.key = requireNonNull(key, "'Key' parameter");
            this.message = requireNonNull(message, "'Message' parameter");
        }

        public StreamMappingAdapter(Map.Entry<StreamKey, InnerMessageId> entry) {
            this(entry.getKey(), entry.getValue());
        }

        public StreamKey getKey() {
            return key;
        }

        public InnerMessageId getMessage() {
            return message;
        }
    }
}
