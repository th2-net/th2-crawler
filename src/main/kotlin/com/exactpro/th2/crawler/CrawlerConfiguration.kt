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

package com.exactpro.th2.crawler

import com.exactpro.th2.crawler.filters.NameFilter
import com.fasterxml.jackson.annotation.JsonProperty
import io.grpc.internal.GrpcUtil
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

class CrawlerConfiguration @JvmOverloads constructor(
    val from: String,
    val to: String? = null,
    val name: String,
    val type: DataType = DataType.EVENTS,
    val defaultLength: String = "PT1H",
    val lastUpdateOffset: Long = 1,
    val lastUpdateOffsetUnit: ChronoUnit = ChronoUnit.HOURS,
    val delay: Long = 10,
    val toLag: Int = 1,
    val toLagOffsetUnit: ChronoUnit = ChronoUnit.HOURS,
    val workAlone: Boolean = false,
    val sessionAliases: Set<String> = emptySet(),
    val maxOutgoingDataSize: Int = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE,

    val filter: NameFilter? = null,

    val shutdownTimeout: Long = 10,
    val shutdownTimeoutUnit: TimeUnit = TimeUnit.SECONDS,

    val useGroupsForRequest: Boolean = false,
    val initialGrpcRequest: Int = 1000, //TODO: add to readme
    val periodicalGrpcRequest: Int = 300, //TODO: add to readme

    val debug: DebugConfiguration = DebugConfiguration() //TODO: add to readme
)

class DebugConfiguration @JvmOverloads constructor(
    val enableProcessor: Boolean = true,
    val enableMessageSizeMeasuring: Boolean = false,
    val enableHandling: Boolean = true,
    val enablePeriodicalGrpcRequest: Boolean = true,
    val enableBackpressure: Boolean = false
)