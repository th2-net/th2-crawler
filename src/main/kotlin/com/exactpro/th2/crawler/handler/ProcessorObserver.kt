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

import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.crawler.Continuation
import com.exactpro.th2.crawler.InternalInterval
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerRequest
import com.exactpro.th2.crawler.dataprocessor.grpc.HandshakeRequest.DataType.MESSAGE
import com.exactpro.th2.crawler.dataprocessor.grpc.HandshakeResponse
import com.exactpro.th2.crawler.dataprocessor.grpc.ProcessorResponse
import com.exactpro.th2.crawler.dataprocessor.grpc.ProcessorResponse.KindCase.HANDLE_REPORT
import com.exactpro.th2.crawler.dataprocessor.grpc.ProcessorResponse.KindCase.HANDSHAKE
import com.exactpro.th2.crawler.dataprocessor.grpc.ProcessorResponse.KindCase.STORE_STATE
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.COMPLETE
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.NEXT_INTERVAL
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.PROCESS
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.WAIT_HANDSHAKE
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.WAIT_INTERVAL
import com.exactpro.th2.crawler.handler.ProcessorObserver.State.WAIT_START
import com.exactpro.th2.crawler.metrics.CrawlerMetrics
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsResponse
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

//FIXME: pass event butcher
class ProcessorObserver(
    private val executor: ScheduledExecutorService,
    private val metrics: CrawlerMetrics,
    private val initProcessing: (StreamObserver<ProcessorResponse>) -> StreamObserver<CrawlerRequest>,
    private val nextInterval: (name: String, version: String) -> Duration,
    private val storeState: (InternalInterval, Continuation) -> Unit,
) : StreamObserver<ProcessorResponse> {

    private val lock = ReentrantLock()

    private var state = WAIT_START
    private lateinit var requestObserver: StreamObserver<CrawlerRequest>
    private lateinit var handshakeResponse: HandshakeResponse

    private lateinit var currentInterval: InternalInterval
    private lateinit var processingController: MessageProcessingController
    private lateinit var startTimestamp: Timestamp
    private lateinit var endTimestamp: Timestamp

    fun start() = lock.withLock {
        check(state == WAIT_START) {
            "Process can't be started because current state is $state instead of $WAIT_START"
        }

        initProcessing(this).apply {
            requestObserver = this
            onNext(CrawlerRequest.newBuilder().apply {
                handshakeBuilder.apply {
                    dataType = MESSAGE
                }
            }.build())
        }
        state = WAIT_HANDSHAKE
    }

    fun stop() = lock.withLock {
        check(state != WAIT_START) {
            "Observer can't be stopped before started"
        }

        if (state == COMPLETE) {
            return@withLock
        }

        requestObserver.onCompleted()
        state = COMPLETE
    }

    override fun onNext(value: ProcessorResponse) {
        when (value.kindCase) {
            HANDSHAKE -> lock.withLock {
                metrics.processorMethodInvoked(ProcessorMethod.CRAWLER_CONNECT)
                LOGGER.info {"Handshake ${shortDebugString(value.handshake)}" }
                check(state == WAIT_HANDSHAKE) {
                    "Process can't be switch to the $PROCESS state because previous state is $state instead of $WAIT_HANDSHAKE"
                }

                handshakeResponse = value.handshake
                requestNextInterval()
            }
            HANDLE_REPORT -> lock.withLock {
                LOGGER.debug {"Handle report ${shortDebugString(value.handleReport)}" }
                check(state == PROCESS || state == WAIT_INTERVAL) {
                    "Report from processor can't be handled, expected $PROCESS or $WAIT_INTERVAL state but actual $state"
                }

                if(processingController.dec(value.handleReport.messagesInfoList)) {
                    requestNextInterval()
                }
            }
            STORE_STATE -> lock.withLock {
                LOGGER.debug {"State storage ${shortDebugString(value.handleReport)}" }
                check(state == PROCESS || state == WAIT_INTERVAL) {
                    "Store request from processor can't be handled, expected $PROCESS or $WAIT_INTERVAL state but actual $state"
                }

//                storeState(currentInterval, value.storeState) //FIXME: implement
            }
            else -> error("Unknown kind of data `${value.kindCase}`")
        }
    }

    override fun onError(t: Throwable) = lock.withLock {
        LOGGER.error(t) { "Processor throw error" }
        state = COMPLETE
    }

    override fun onCompleted() = lock.withLock {
        LOGGER.info("Processor complete interaction")
        state = COMPLETE
    }

    fun onProviderResponse(response: CradleMessageGroupsResponse) {
        if (response.hasMessageIntervalInfo()) {
            lock.withLock {
                check(state == WAIT_INTERVAL) {
                    "Process can't be started because current state is $state instead of $WAIT_INTERVAL"
                }

                if(processingController.onProviderResponse(response.messageIntervalInfo)) {
                    requestNextInterval()
                } else {
                    state = PROCESS
                }
            }
        }
    }

    fun onStartInterval(interval: InternalInterval) = lock.withLock {
        metrics.processorMethodInvoked(ProcessorMethod.INTERVAL_START)
        check(state == NEXT_INTERVAL) {
            "Interval can not be started because state is $state instead of $NEXT_INTERVAL"
        }
        this.currentInterval = interval
        requestObserver.onNext(CrawlerRequest.newBuilder().apply {
            intervalStartBuilder.apply {
                interval.original.startTime.toTimestamp().also {
                    startTime = it
                    startTimestamp = it
                }
                interval.original.endTime.toTimestamp().also {
                    endTime = it
                    endTimestamp = it
                }
            }
        }.build())
        state = WAIT_INTERVAL
    }

    private fun requestNextInterval() {
        check(!::processingController.isInitialized || processingController.isComplete) {
            "Previous interval isn't fully processed, $processingController"
        }

        if (::processingController.isInitialized) {
            requestObserver.onNext(CrawlerRequest.newBuilder().apply {
                intervalEndBuilder.apply {
                    startTime = startTimestamp
                    endTime = endTimestamp
                }
            }.build())
        }

        processingController = MessageProcessingController()
        executor.submit(::nextIntervalTask)
        state = NEXT_INTERVAL
    }

    private fun nextIntervalTask() {
        lock.withLock {
            check(state != NEXT_INTERVAL) {
                "Observer can not request new interval because current state is $state instead of $NEXT_INTERVAL"
            }

            val duration = nextInterval(handshakeResponse.name, handshakeResponse.version)
            if (duration != Duration.ZERO) {
                executor.schedule(::nextIntervalTask, duration.toMillis(), TimeUnit.MILLISECONDS)
            }
        }
    }

    private enum class State {
        WAIT_START,
        WAIT_HANDSHAKE,
        NEXT_INTERVAL,
        WAIT_INTERVAL,
        PROCESS,
        COMPLETE,
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}