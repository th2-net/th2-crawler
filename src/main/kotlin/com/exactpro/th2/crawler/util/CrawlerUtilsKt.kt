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

import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import java.util.stream.Stream

fun SearchResult<MessageGroupResponse>.toCompactString(): String {
    val unorderedMessages = data.asSequence()
        .windowed(2, 1)
        .filter { (first, second) -> first.messageId.timestamp.toInstant().isAfter(second.messageId.timestamp.toInstant()) }
        .map { (first, second) -> "${first.extractIdWithTimestamp()} - ${second.extractIdWithTimestamp()}" }
        .toList()

    val timestamps = data.map { it.messageId.timestamp.toInstant() }

    val streams = data
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
                .let { if (it.isNotEmpty()) "gaps=$it" else "" }

            val messageTimestamps = elements.map { it.messageId.timestamp.toInstant() }
            """
                |    $sessionAlias:$direction
                |      sequences: $min..$max count=${sequences.size} $gaps
                |      timestamps: ${messageTimestamps.minOrNull()}..${messageTimestamps.maxOrNull()}
            """.trimMargin()
        }

    val builder = StringBuilder("""
        Search result: 
          messages: count=${data.size}
          timestamps: ${timestamps.minOrNull()}..${timestamps.maxOrNull()} 
    """.trimIndent())
    if (unorderedMessages.isNotEmpty()) {
        builder.append(System.lineSeparator())
        builder.append("""
            |  unordered messages:
            |    ${unorderedMessages.joinToString("\n    ")}
        """.trimMargin())
    }
    builder.append(System.lineSeparator())
        .append("  streams:")
        .append(System.lineSeparator())
    builder.append(streams.joinToString(System.lineSeparator()))

    return builder.toString()
}

fun Stream<Timestamp>.maxOrDefault(default: Timestamp): Timestamp
    = sorted { timestampA, timestampB -> Timestamps.compare(timestampA, timestampB) }
        .findFirst().orElse(default)
private fun MessageGroupResponse.extractIdWithTimestamp() = "${messageId.connectionId.sessionAlias}:${messageId.direction}:${messageId.sequence}(${messageId.timestamp.toInstant()})"