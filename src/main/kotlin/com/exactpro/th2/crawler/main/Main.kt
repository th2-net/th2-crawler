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
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.crawler.CrawlerConfiguration
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import mu.KotlinLogging
import kotlin.system.exitProcess

import java.io.IOException

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) {
    LOGGER.info { "Starting com.exactpro.th2.crawler.Crawler" }

    var factory: CommonFactory? = null
    try {
        factory = CommonFactory.createFromArguments(*args)

        val grpcRouter = factory.grpcRouter
        val dataProcessor = grpcRouter.getService(DataProcessorService::class.java)
        val dataProviderService = grpcRouter.getService(DataProviderService::class.java)

        val cradleManager = factory.cradleManager

        val configuration = factory.getCustomConfiguration(CrawlerConfiguration::class.java)

        // The BOX is alive
        liveness = true

        val crawler = Crawler(
            cradleManager.storage,
            dataProcessor,
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
    } catch (ex: UnexpectedDataProcessorException) {
        LOGGER.info("Data processor changed its name and/or version", ex)
        exitProcess(0)
    } catch (ex: UpdateNotAppliedException) {
        LOGGER.info("Failed to update some fields of table with intervals", ex)
        exitProcess(0)
    } catch (ex: Exception) {
        LOGGER.error("Cannot start Crawler", ex)
        exitProcess(1)
    } finally {
        readiness = false
        runCatching { factory?.close() }.onFailure { LOGGER.error(it) { "Cannot close common factory" } }
        liveness = false
    }
}