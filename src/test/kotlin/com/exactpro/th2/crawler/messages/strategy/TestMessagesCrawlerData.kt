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
import com.exactpro.th2.crawler.CrawlerManager.BOOK_NAME
import com.exactpro.th2.crawler.createMessageID
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId
import com.exactpro.th2.crawler.state.v2.StreamKey
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointers
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant

class TestMessagesCrawlerData {
    private val timestamp: Instant = Instant.now()
    private val responses: Collection<MessageSearchResponse> =
        ArrayList<MessageSearchResponse>().apply {
            repeat(10) {
                add(message(it.toLong(), timestamp = timestamp))
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
            responses.iterator(), emptyMap(), CrawlerId.newBuilder()
                .setName("test")
                .build(),
            oneMessageSize * 2 + 10
        ) { true }

        val dataParts = data.asSequence().toList()
        assertInOrder(dataParts)
        Assertions.assertEquals(10, dataParts.size)
        val continuation: MessagesCrawlerData.ResumeMessageIDs? = data.continuation
        Assertions.assertNotNull(continuation) { "continuation is null" }
        continuation!!
        Assertions.assertEquals(
            mapOf(
                StreamKey(BOOK_NAME, "test", Direction.FIRST) to MessageID.newBuilder()
                    .setDirection(Direction.FIRST)
                    .setBookName(BOOK_NAME)
                    .setSequence(9)
                    .setTimestamp(timestamp.toTimestamp())
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

    private fun message(
        sequence: Long,
        alias: String = "test",
        name: String = "test_message",
        timestamp: Instant = Instant.now()
    ): MessageSearchResponse {
        val direction = Direction.FIRST
        val messageID = createMessageID(alias, direction, sequence, timestamp = timestamp)
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
                .build())
            .build()
    }
}