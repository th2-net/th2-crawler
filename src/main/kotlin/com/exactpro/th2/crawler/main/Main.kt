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
import com.exactpro.th2.crawler.Crawler
import com.exactpro.th2.common.metrics.liveness
import com.exactpro.th2.common.metrics.readiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.crawler.CrawlerConfiguration
import com.exactpro.th2.crawler.dataservice.grpc.DataServiceService
import com.exactpro.th2.crawler.exception.UnexpectedDataServiceException
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import mu.KotlinLogging
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.system.exitProcess

import java.io.IOException

private val LOGGER = KotlinLogging.logger { }

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
    val lock = ReentrantLock()
    val condition: Condition = lock.newCondition()
    configureShutdownHook(resources, lock, condition)

    try {
        // You need to initialize the CommonFactory

        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resources += factory

        val cradleManager = factory.cradleManager

        val grpcRouter = factory.grpcRouter

        resources += grpcRouter

        val dataService = grpcRouter.getService(DataServiceService::class.java)
        val dataProviderService = grpcRouter.getService(DataProviderService::class.java)

        val configuration = factory.getCustomConfiguration(CrawlerConfiguration::class.java)

        // The BOX is alive
        liveness = true

        // Do additional initialization required to your logic

        val crawler = Crawler(
            cradleManager.storage,
            dataService,
            dataProviderService,
            configuration
        )

        // The BOX is ready to work
        readiness = true

        LOGGER.info { "Crawler was created and is going to start" }

        while (!Thread.currentThread().isInterrupted) {
            val sleepTime = crawler.process()

            Thread.sleep(sleepTime.toMillis())
        }

        LOGGER.info { "Crawler is going to shutdown" }

    } catch (ex: IOException) {
        LOGGER.error("Error while interacting with Cradle in Crawler", ex)
        exitProcess(1)
    } catch (ex: InterruptedException) {
        LOGGER.error("Crawler's sleep was interrupted", ex)
        Thread.currentThread().interrupt()
        exitProcess(1)
    } catch (ex: UnexpectedDataServiceException) {
        LOGGER.info("Data service changed its name and/or version", ex)
        exitProcess(0)
    } catch (ex: UpdateNotAppliedException) {
        LOGGER.info("Failed to update some fields of table with intervals", ex)
        exitProcess(0)
    } catch (ex: Exception) {
        LOGGER.error("Cannot start Crawler", ex)
        exitProcess(1)
    } finally {
        liveness = false
        readiness = false
    }
}

private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
    Runtime.getRuntime().addShutdownHook(thread(
        start = false,
        name = "Shutdown hook"
    ) {
        LOGGER.info { "Shutdown start" }
        readiness = false
        try {
            lock.lock()
            condition.signalAll()
        } finally {
            lock.unlock()
        }
        resources.descendingIterator().forEachRemaining { resource ->
            try {
                resource.close()
            } catch (e: Exception) {
                LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
        liveness = false
        LOGGER.info { "Shutdown end" }
    })
}

