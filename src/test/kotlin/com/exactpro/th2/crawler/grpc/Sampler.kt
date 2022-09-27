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

package com.exactpro.th2.crawler.grpc

import mu.KotlinLogging
import java.text.DecimalFormat
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Sampler(
    private val name: String,
    private val samplingFrequency: Long,
) {
    private val lock = ReentrantLock()
    private var start = System.nanoTime()
    private var count = 0.0
    private var totalCount = 0

    private val measurements = mutableListOf<Double>()

    fun inc(BATCH_SIZE: Int) = lock.withLock {
        totalCount += BATCH_SIZE
        count += BATCH_SIZE
        if (count >= samplingFrequency) {
            System.nanoTime().also {
                ((count / (it - start)) * 1_000_000_000).also { measurement ->
                    log(measurement)
                    measurements.add(measurement)
                    start = it
                    count = 0.0
                }
            }
        }
    }

    fun complete() = lock.withLock {
        with(measurements) {
            sort()
            log(get(size / 2), "complete")
        }
    }

    private fun log(measurement: Double, comment: String = "") {
        LOGGER.info { "$name $comment ${NUMBER_FORMAT.format(measurement)} msg/sec. $totalCount total msg" }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val NUMBER_FORMAT = DecimalFormat("############.###")
    }
}