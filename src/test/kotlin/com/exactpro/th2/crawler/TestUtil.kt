/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.crawler

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.crawler.state.v1.InnerEventId
import com.exactpro.th2.crawler.state.v1.InnerMessageId
import com.exactpro.th2.crawler.state.v1.RecoveryState
import com.exactpro.th2.crawler.state.v1.StreamKey
import java.time.Instant

@JvmOverloads
fun createMessageID(sessionAlias: String, direction: Direction, sequence: Long, subSequence: List<Int> = emptyList()): MessageID = MessageID.newBuilder().apply {
    connectionIdBuilder.apply {
        this.sessionAlias = sessionAlias
    }
    this.direction = direction
    this.sequence = sequence
    addAllSubsequence(subSequence)
}.build()

fun createRecoveryState() = RecoveryState(
    InnerEventId(
        Instant.now(),
        "event_id"
    ),
    mapOf(
        StreamKey("test", Direction.FIRST) to InnerMessageId(Instant.now(), 42L),
        StreamKey("test", Direction.SECOND) to InnerMessageId(Instant.now(), 43L)
    ),
    10,
    15
)