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

package com.exactpro.th2.crawler.messages.strategy

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.crawler.createMessageID
import com.exactpro.th2.crawler.state.v1.StreamKey
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

class MessageStrategyTest {

    @Test
    fun associateWithStreamKeyTest() {
        val messages = listOf(
            createMessageID("A", FIRST, 1),
            createMessageID("A", FIRST, 2),
            createMessageID("A", SECOND, 10, listOf(2)),
            createMessageID("A", SECOND, 10, listOf(1)),
        )

        val resultLatest: Map<StreamKey, MessageID> = MessagesStrategy.associateWithStreamKey(messages.stream(), MessagesStrategy.LATEST_SEQUENCE)

        assertAll(
            { Assertions.assertEquals(2, resultLatest.size) },
            { messages[1].assertValue(resultLatest) },
            { messages[2].assertValue(resultLatest) },
        )

        val resultEarlest: Map<StreamKey, MessageID> = MessagesStrategy.associateWithStreamKey(messages.stream(), MessagesStrategy.EARLIEST_SEQUENCE)

        assertAll(
            { Assertions.assertEquals(2, resultEarlest.size) },
            { messages[0].assertValue(resultEarlest) },
//            { messages[3].assertValue(resultEarlest) }, //FIXME: the associateWithStreamKey must consider subsequence
        )
    }

    private fun MessageID.assertValue(
        result: Map<StreamKey, MessageID>
    ) {
        val sessionKey = MessagesStrategy.createStreamKeyFrom(this)
        Assertions.assertEquals(this, result[sessionKey])
    }
}