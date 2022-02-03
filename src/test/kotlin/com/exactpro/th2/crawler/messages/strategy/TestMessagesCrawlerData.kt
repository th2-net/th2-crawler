/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId
import com.exactpro.th2.crawler.state.v1.StreamKey
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestMessagesCrawlerData {
    private val responses: Collection<StreamResponse> =
        ArrayList<StreamResponse>().apply {
            repeat(10) {
                add(message(it.toLong()))
            }
            val allIds = this.mapNotNull { if (it.hasMessage()) it.message.messageId else null }
            add(
                StreamResponse.newBuilder()
                    .setStreamInfo(StreamsInfo.newBuilder()
                        .addAllStreams(allIds.groupBy { it.run { connectionId.sessionAlias to direction } }.map { (key, ids) ->
                            val (alias, direction) = key
                            Stream.newBuilder()
                                .setDirection(direction)
                                .setSession(alias)
                                .setLastId(requireNotNull(ids.maxByOrNull { it.sequence }))
                                .build()
                        })
                    )
                    .build()
            )
        }

    @Test
    fun `returns correct data parts`() {
        val oneMessageSize = responses.first().message.serializedSize
        val data = MessagesCrawlerData(
            responses.iterator(), emptyMap(), CrawlerId.newBuilder()
                .setName("test")
                .build(),
            100,
            oneMessageSize * 2 /*2 msg per request*/ + oneMessageSize / 2 /*for request*/
        ) { true }

        val dataParts = data.asSequence().toList()
        Assertions.assertEquals(5, dataParts.size)
        val continuation: MessagesCrawlerData.ResumeMessageIDs? = data.continuation
        Assertions.assertNotNull(continuation) { "continuation is null" }
        continuation!!
        Assertions.assertEquals(
            mapOf(
                StreamKey("test", Direction.FIRST) to MessageID.newBuilder()
                    .setDirection(Direction.FIRST)
                    .setSequence(9)
                    .setConnectionId(ConnectionID.newBuilder().setSessionAlias("test"))
                    .build()
            ),
            continuation.ids
        )
    }

    private fun message(sequence: Long, alias: String = "test", name: String = "test_message"): StreamResponse {
        val direction = Direction.FIRST
        val messageID = MessageID.newBuilder()
            .setDirection(direction)
            .setSequence(sequence)
            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(alias))
            .build()
        return StreamResponse.newBuilder()
            .setMessage(MessageData.newBuilder()
                .setDirection(direction)
                .setMessageId(messageID)
                .setMessage(Message.newBuilder().apply {
                    metadataBuilder.apply {
                        id = messageID
                        messageType = name
                    }
                })
                .setSessionId(messageID.connectionId)
                .setMessageType(name)
                .build())
            .build()
    }
}