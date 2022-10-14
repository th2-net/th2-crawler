/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.crawler.handler

import com.exactpro.th2.crawler.state.v1.StreamKey
import com.exactpro.th2.dataprovider.grpc.MessageIntervalInfo
import com.exactpro.th2.dataprovider.grpc.MessageStreamInfo
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.concurrent.ThreadSafe
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.abs

@ThreadSafe
class MessageProcessingController {
    private val lock = ReentrantReadWriteLock()
    private val remaining = hashMapOf<StreamKey, Long>()
    private var initialised: Boolean = false

    val isComplete: Boolean
        get() = lock.read { initialised && remaining.isEmpty() }

    fun dec(streamInfos: Collection<MessageStreamInfo>): Boolean = lock.write {
        streamInfos.groupBy { StreamKey(it.sessionAlias, it.direction) }
            .forEach { (streamId, list) ->
                val sum = list.asSequence()
                    .map(MessageStreamInfo::getNumberOfMessages)
                    .sum()

                remaining.compute(streamId) { _, previous ->
                    val result = (previous ?: 0L) - sum
                    when {
                        initialised && result < 0L -> error("Processor has processed unexpected ${abs(result)} messages for the $streamId stream")
                        result == 0L -> null
                        else -> result
                    }
                }
            }

        isComplete.also {
            LOGGER.debug { "Complete status is $it after decrement $remaining" }
        }
    }

    fun onProviderResponse(intervalInfo: MessageIntervalInfo): Boolean = lock.write {
        intervalInfo.messagesInfoList.asSequence()
            .filterNot { StringUtils.isBlank(it.sessionAlias) }
            .forEach { streamInfo ->
            remaining.compute(StreamKey(streamInfo.sessionAlias, streamInfo.direction)) { streamId, previous ->
                val result = (previous ?: 0L) + streamInfo.numberOfMessages
                when {
                    result < 0L -> error("Processor has processed unexpected ${abs(result)} messages for the $streamId stream")
                    result == 0L -> null
                    else -> result
                }
            }
        }

        initialised = true
        isComplete.also {
            LOGGER.debug { "Complete status is $it after provider response $remaining" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

