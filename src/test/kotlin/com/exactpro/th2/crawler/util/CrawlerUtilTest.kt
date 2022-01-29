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

package com.exactpro.th2.crawler.util

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant

class CrawlerUtilTest {

    @Test
    fun `search result of message data toCompactString test`() {
        val compactString = SearchResult<MessageData>(listOf(
            /*
             A:FIRST ordered
             A:SECOND only one
             B:FIRST has a gap
             C:FIRST unorderd internal
             D:SECOND unorderd with A
             */
            createMessageData("A", FIRST, 1, "1970-01-01T00:00:00Z"),
            createMessageData("D", SECOND, 50, "1970-01-01T00:00:10Z"),
            createMessageData("A", SECOND, 10, "1970-01-01T00:00:01Z"),
            createMessageData("B", FIRST, 30, "1970-01-01T00:00:01Z"),
            createMessageData("A", FIRST, 2, "1970-01-01T00:00:03Z"),
            createMessageData("B", FIRST, 32, "1970-01-01T00:00:04Z"),
            createMessageData("C", FIRST, 41, "1970-01-01T00:00:06Z"),
            createMessageData("C", FIRST, 42, "1970-01-01T00:00:05Z"),
        ), StreamsInfo.getDefaultInstance()).toCompactString()

        Assertions.assertEquals("""
            Search result: 
              messages: count=8
              timestamps: 1970-01-01T00:00:00Z..1970-01-01T00:00:10Z 
              unordered messages:
                D:SECOND:50(1970-01-01T00:00:10Z) - A:SECOND:10(1970-01-01T00:00:01Z)
                C:FIRST:41(1970-01-01T00:00:06Z) - C:FIRST:42(1970-01-01T00:00:05Z)
              streams:
                A:FIRST
                  sequences: 1..2 count=2 
                  timestamps: 1970-01-01T00:00:00Z..1970-01-01T00:00:03Z
                D:SECOND
                  sequences: 50..50 count=1 
                  timestamps: 1970-01-01T00:00:10Z..1970-01-01T00:00:10Z
                A:SECOND
                  sequences: 10..10 count=1 
                  timestamps: 1970-01-01T00:00:01Z..1970-01-01T00:00:01Z
                B:FIRST
                  sequences: 30..32 count=2 gaps=[[30, 32]]
                  timestamps: 1970-01-01T00:00:01Z..1970-01-01T00:00:04Z
                C:FIRST
                  sequences: 41..42 count=2 
                  timestamps: 1970-01-01T00:00:05Z..1970-01-01T00:00:06Z
        """.trimIndent(), compactString)
    }

    private fun createMessageData(sessionAlias: String, direction: Direction, sequence: Long, timestamp: String) = MessageData.newBuilder().apply {
        this.timestamp = Instant.parse(timestamp).toTimestamp()
        messageIdBuilder.apply {
            this.direction = direction
            this.sequence = sequence
            connectionIdBuilder.apply {
                this.sessionAlias = sessionAlias
            }
        }
    }.build()
}