/*
 *  Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.intervals.Interval
import com.exactpro.cradle.intervals.IntervalsWorker
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo
import com.exactpro.th2.crawler.metrics.CrawlerMetrics
import com.exactpro.th2.crawler.state.StateService
import com.exactpro.th2.crawler.state.v1.RecoveryState
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.google.protobuf.Timestamp
import mu.KotlinLogging
import java.io.IOException

interface DataTypeStrategy<T : CrawlerData<C>, C : Continuation> {
    fun setupIntervalInfo(info: IntervalInfo.Builder, state: RecoveryState?)

    /**
     * @param state The current state stored in cradle
     * @return the continuation of type [C] with information required for requesting data
     */
    fun continuationFromState(state: RecoveryState): C?

    /**
     * @param current The current state stored in cradle or `null` if nothing is stored yet
     * @param continuation the continuation with information about the current position in data stream
     * @param processedData the total amount of the processed data for current interval
     * @return a recovery state that aggregates information from [current] state and [continuation]
     */
    fun continuationToState(current: RecoveryState?, continuation: C, processedData: Long): RecoveryState

    /**
     * @param start the lower boundary for requested data
     * @param end the upper boundary for requested data
     * @param parameters parameters for the request
     * @param continuation the continuation from previously requested data or `null` if data should be requested from the beginning
     * @return the [CrawlerData] object with data to process
     */
    fun requestData(
        start: Timestamp,
        end: Timestamp,
        parameters: DataParameters,
        continuation: C?
    ): T

    /**
     * @param processor the processor to transfer data
     * @param interval the current interval we are working on
     * @param parameters parameters which was used to request data
     * @param data the data to process
     * @return the report with information about next action and the checkpoint to store in [RecoveryState]
     */
    fun processData(
        processor: DataProcessorService,
        interval: InternalInterval,
        parameters: DataParameters,
        data: T
    ): Report<C>
}

interface CrawlerData<C : Continuation> {
    val isNeedsNextRequest: Boolean
    val hasData: Boolean
    val continuation: C?
    fun size(): Int
}

interface Continuation

interface DataTypeStrategyFactory<T : CrawlerData<C>, C : Continuation> {
    val dataType: DataType
    fun create(
        worker: IntervalsWorker,
        provider: DataProviderService,
        stateService: StateService<RecoveryState>,
        metrics: CrawlerMetrics,
        config: CrawlerConfiguration, // TODO: maybe use a separate class
    ): DataTypeStrategy<T, C>
}

class DataParameters(
    val dataProcessorInfo: DataProcessorInfo,
    val crawlerId: CrawlerId,
    val sessionAliases: Collection<String> = emptyList(),
)

data class Report<out C>(
    val action: Action,
    val processedData: Long,
    val remainingData: Long,
    val checkpoint: C? = null
) {
    companion object {
        private val HANDSHAKE = Report<Nothing>(Action.HANDSHAKE, 0, 0)

        private val EMPTY = Report<Nothing>(Action.CONTINUE, 0, 0)

        @JvmStatic
        fun <C> handshake(): Report<C> = HANDSHAKE

        @JvmStatic
        fun <C> empty(): Report<C> = EMPTY
    }
}

class InternalInterval(
    private val stateService: StateService<RecoveryState>,
    interval: Interval,
) {
    var original: Interval = interval
        private set
    private var _state: RecoveryState? = null
    val state: RecoveryState?
        get() = when (_state) {
            null -> stateService.deserialize(original.recoveryState).also { _state = it }
            else -> _state
        }

    @Throws(IOException::class)
    fun updateState(newState: RecoveryState, worker: IntervalsWorker) {
        val serializedState = stateService.serialize(newState)
        LOGGER.trace { "Updating state for interval ${original.toLogString()}: $serializedState" }
        original = worker.updateRecoveryState(original, serializedState)
        _state = newState
    }

    @Throws(IOException::class)
    fun processed(processed: Boolean, worker: IntervalsWorker) {
        LOGGER.trace { "Updating processed status for interval ${original.toLogString()}: value=$processed" }
        original = worker.setIntervalProcessed(original, processed)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private fun Interval.toLogString(): String = "(from: $startTime; to: $endTime)"
    }
}

enum class Action { CONTINUE, HANDSHAKE }
