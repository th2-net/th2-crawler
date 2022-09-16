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
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.crawler.CrawlerConfiguration
import com.exactpro.th2.crawler.createMessageID
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId
import com.exactpro.th2.crawler.metrics.CrawlerMetrics
import com.exactpro.th2.crawler.state.v1.StreamKey
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointers
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.time.Instant

@Disabled
class TestMessagesCrawlerData {
    private val responses: Collection<MessageSearchResponse> =
        ArrayList<MessageSearchResponse>().apply {
            repeat(10) {
                add(message(it.toLong()))
            }
            val allIds = this.mapNotNull { if (it.hasMessage()) it.message.messageId else null }
            add(
                MessageSearchResponse.newBuilder()
                    .setMessageStreamPointers(MessageStreamPointers.newBuilder()
                        .addAllMessageStreamPointer(allIds.groupBy { it.run { connectionId.sessionAlias to direction } }.map { (key, ids) ->
                            val (alias, direction) = key
                            MessageStreamPointer.newBuilder()
                                .setMessageStream(MessageStream.newBuilder().setName(alias).setDirection(direction))
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
            mock(CrawlerMetrics::class.java),
            CrawlerConfiguration(from = "", name = "", maxOutgoingDataSize = oneMessageSize * 2 + 10),
            responses.iterator(), emptyMap(), CrawlerId.newBuilder()
                .setName("test")
                .build()
        ) { true }

        val dataParts = data.asSequence().toList()
        assertInOrder(dataParts)
        Assertions.assertEquals(10, dataParts.size)
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

    private fun assertInOrder(dataParts: List<MessagesCrawlerData.MessagePart>) {
        var lastSeq = -1L
        for (part in dataParts) {
            for (msg in part.request.messageDataList) {
                val sequence = msg.messageId.sequence
                Assertions.assertTrue(lastSeq < sequence) { "Unordered sequences: $lastSeq and $sequence" }
                lastSeq = sequence
            }
        }
    }

    private fun message(sequence: Long, alias: String = "test", name: String = "test_message"): MessageSearchResponse {
        val direction = Direction.FIRST
        val messageID = createMessageID(alias, direction, sequence)
        return MessageSearchResponse.newBuilder()
            .setMessage(
                MessageGroupResponse.newBuilder()
                .setMessageId(messageID)
                    .addMessageItem(MessageGroupItem.newBuilder()
                        .setMessage(Message.newBuilder().apply {
                            metadataBuilder.apply {
                                id = messageID
                                messageType = name
                            }
                        })
                    )
                .setTimestamp(Instant.now().toTimestamp())
                .build())
            .build()
    }
}