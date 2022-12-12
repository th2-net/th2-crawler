/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.crawler.state.v2

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter
import java.util.*
import java.util.stream.Collectors

class StreamKeyMapConverter {
    class Serialize : StdConverter<Map<StreamKey?, InnerMessageId?>?, List<StreamMappingAdapter>?>() {
        override fun convert(value: Map<StreamKey?, InnerMessageId?>?): List<StreamMappingAdapter>? {
            return value?.entries?.stream()
                ?.map { entry: Map.Entry<StreamKey?, InnerMessageId?> -> StreamMappingAdapter(entry) }
                ?.collect(Collectors.toUnmodifiableList())
        }
    }

    class Deserialize : StdConverter<List<StreamMappingAdapter>, Map<StreamKey, InnerMessageId>>() {
        override fun convert(value: List<StreamMappingAdapter>?): Map<StreamKey, InnerMessageId>? {
            return value?.asSequence()
                ?.map { it.key to it.message }
                ?.toMap()
        }
    }

    class StreamMappingAdapter @JsonCreator constructor(
        @JsonProperty("key") key: StreamKey?,
        @JsonProperty("message") message: InnerMessageId?
    ) {
        val key: StreamKey = requireNotNull(key) { "'Key' parameter" }
        val message: InnerMessageId = requireNotNull(message) { "'Message' parameter" }

        constructor(entry: Map.Entry<StreamKey?, InnerMessageId?>) : this(entry.key, entry.value)
    }
}