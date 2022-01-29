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
@file:JvmName("CrawlerUtilKt")
package com.exactpro.th2.crawler.util

import com.exactpro.th2.dataprovider.grpc.MessageData
import com.google.protobuf.Timestamp
import java.time.Instant

fun SearchResult<MessageData>.toCompactString(): String {
    val unorderedMessages = data.asSequence()
        .windowed(2, 1)
        .filter { (first, second) -> first.timestamp.toInstant().isAfter(second.timestamp.toInstant()) }
        .map { (first, second) -> "${first.extractIdWithTimstamp()} - ${second.extractIdWithTimstamp()}" }
        .toList()
        .let { if (it.isNotEmpty()) "unordered messages:\n${it.joinToString("\n  ", "  ","\n")}" else "" }

    return data
        .groupBy { Pair(it.messageId.connectionId.sessionAlias, it.messageId.direction) }
        .map { (sessionKey, elements) ->
            val (sessionAlias, direction) = sessionKey

            val sequences = elements.map { it.messageId.sequence }
            val min = sequences.minOrNull() ?: Long.MIN_VALUE
            val max = sequences.maxOrNull() ?: Long.MAX_VALUE
            val gaps = sequences.asSequence()
                .windowed(2, 1)
                .filter { (first, second) -> first + 1 != second }
                .toList()
                .let { if(it.isNotEmpty()) "gaps=$it" else "" }

            val timestamps = elements.map { it.timestamp.toInstant() }
            """
                $sessionAlias:$direction
                  min=$min max=$max $gaps
                  early=${timestamps.minOrNull()} late=${timestamps.maxOrNull()}
            """.trimIndent()
        }.joinToString(
            prefix = unorderedMessages,
            separator = System.lineSeparator()
        )
}

private fun Timestamp.toInstant() = Instant.ofEpochSecond(seconds, nanos.toLong())
private fun MessageData.extractIdWithTimstamp() = "${messageId.connectionId.sessionAlias}:${messageId.direction}:${messageId.sequence}(${timestamp.toInstant()})"