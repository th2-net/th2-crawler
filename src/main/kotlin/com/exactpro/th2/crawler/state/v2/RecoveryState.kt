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

import com.exactpro.th2.crawler.state.BaseState
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.util.*

data class RecoveryState @JsonCreator constructor(
    @param:JsonProperty("lastProcessedEvent") val lastProcessedEvent: InnerEventId?,
    @field:JsonSerialize(converter = StreamKeyMapConverter.Serialize::class)
    @field:JsonDeserialize(converter = StreamKeyMapConverter.Deserialize::class)
    @param:JsonProperty("lastProcessedMessages") val lastProcessedMessages: Map<StreamKey, InnerMessageId>?,
    @param:JsonProperty("lastNumberOfEvents") val lastNumberOfEvents: Long,
    @param:JsonProperty("lastNumberOfMessages") val lastNumberOfMessages: Long
) : BaseState