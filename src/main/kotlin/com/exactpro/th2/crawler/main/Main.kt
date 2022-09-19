/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("Main")

package com.exactpro.th2.crawler.main

import com.exactpro.cradle.utils.UpdateNotAppliedException
import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.crawler.Crawler
import com.exactpro.th2.crawler.CrawlerConfiguration
import com.exactpro.th2.crawler.CrawlerContext
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException
import com.exactpro.th2.crawler.grpc.BlockingService
import com.exactpro.th2.crawler.metrics.impl.PrometheusMetrics
import com.exactpro.th2.crawler.state.StateService
import com.exactpro.th2.crawler.state.v1.RecoveryState
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl
import com.exactpro.th2.dataprovider.grpc.AsyncDataProviderService
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import mu.KotlinLogging
import java.io.IOException
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger { }
private enum class State { WORK, WAIT, STOP }

fun main(args: Array<String>) {
    LOGGER.info { "Starting com.exactpro.th2.crawler.Crawler" }
    // Here is an entry point to the th2-box.

    // Configure shutdown hook for closing all resources
    // and the lock condition to await termination.
    //
    // If you use the logic that doesn't require additional threads
    // and you can run everything on main thread
    // you can omit the part with locks (but please keep the resources queue)
    val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    configureShutdownHook(resources)

    try {
        // You need to initialize the CommonFactory

        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resources += factory

        val cradleManager = factory.cradleManager
        val configuration = factory.getCustomConfiguration(CrawlerConfiguration::class.java)

        val context = CrawlerContext(CrawlerTimeImpl(), PrometheusMetrics(), configuration)

        val grpcRouter = factory.grpcRouter
        val dataProcessor = grpcRouter.getService(DataProcessorService::class.java)
        val dataProviderService = if(configuration.debug.enableBackpressure) {
            BlockingService(context, grpcRouter.getService(AsyncDataProviderService::class.java))
        } else {
            grpcRouter.getService(DataProviderService::class.java)
        }

        // The BOX is alive
        LIVENESS_MONITOR.enable()

        val crawler = Crawler(
            StateService.createFromClasspath(
                dataProvider = dataProviderService,
                // make sure that the default implementation is not changed when you create a new version of state
                defaultImplementation = RecoveryState::class.java,
            ),
            cradleManager.storage,
            dataProcessor,
            dataProviderService,
            context
        )

        // The BOX is ready to work
        READINESS_MONITOR.enable()

        LOGGER.info { "Crawler was created and is going to start" }

        val state = AtomicReference(State.WAIT)
        resources += AutoCloseable {
            with(configuration) {
                awaitCrawlerFinishesProcessing(state, shutdownTimeout, shutdownTimeoutUnit)
            }
        }

        while (!Thread.currentThread().isInterrupted) {
            val sleepTime = try {
                if (state.compareAndSet(State.WAIT, State.WORK)) {
                    crawler.process()
                } else {
                    LOGGER.info { "Crawler has state ${state.get()} that does not allow to start processing" }
                    return
                }
            } finally {
                state.set(State.WAIT)
            }

            Thread.sleep(sleepTime.toMillis())
        }

        LOGGER.info { "Crawler is going to shutdown" }

    } catch (ex: IOException) {
        LOGGER.error(ex) { "Error while interacting with Cradle in Crawler" }
        exitProcess(1)
    } catch (ex: InterruptedException) {
        LOGGER.error(ex) { "Crawler's sleep was interrupted" }
        Thread.currentThread().interrupt()
        exitProcess(1)
    } catch (ex: UnexpectedDataProcessorException) {
        LOGGER.info(ex) { "Data processor changed its name and/or version" }
        exitProcess(0)
    } catch (ex: UpdateNotAppliedException) {
        LOGGER.info(ex) { "Failed to update some fields of table with intervals" }
        exitProcess(0)
    } catch (ex: Exception) {
        LOGGER.error(ex) { "Cannot start Crawler" }
        exitProcess(1)
    }
}

private fun awaitCrawlerFinishesProcessing(
    state: AtomicReference<State>,
    timeout: Long,
    unit: TimeUnit,
) {
    LOGGER.info { "Awaiting the crawler finishes current processing" }

    val switchState: () -> Boolean = { state.compareAndSet(State.WAIT, State.STOP) }

    if (!switchState()) {
        val awaitTime = System.currentTimeMillis() + unit.toMillis(timeout)
        while (!switchState() && System.currentTimeMillis() < awaitTime) {
            Thread.sleep(100) // await processing finished
        }
    }
    if (state.get() == State.STOP) {
        LOGGER.info { "Crawler has finished processing the current interval" }
    } else {
        LOGGER.warn { "Crawler did not finish processing the current interval in $timeout $unit" }
    }
}

private fun configureShutdownHook(resources: Deque<AutoCloseable>) {
    Runtime.getRuntime().addShutdownHook(thread(
        start = false,
        name = "Shutdown hook"
    ) {
        LOGGER.info { "Shutdown start" }
        READINESS_MONITOR.disable()
        resources.descendingIterator().forEachRemaining { resource ->
            try {
                resource.close()
            } catch (e: Exception) {
                LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
        LIVENESS_MONITOR.disable()
        LOGGER.info { "Shutdown end" }
    })
}