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

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.temporal.ChronoUnit

class CrawlerConfiguration(
    @JsonProperty
    val from: String,
    @JsonProperty
    val to: String? = null,
    @JsonProperty
    val name: String,
    @JsonProperty
    val type: String = "EVENTS",
    @JsonProperty
    val defaultLength: String = "PT1H",
    @JsonProperty
    val lastUpdateOffset: Long = 1,
    @JsonProperty
    val lastUpdateOffsetUnit: ChronoUnit = ChronoUnit.HOURS,
    @JsonProperty
    val delay: Long = 10,
    @JsonProperty
    val batchSize: Int = 300,
    @JsonProperty
    val toLag: Int = 1,
    @JsonProperty
    val toLagOffsetUnit: ChronoUnit = ChronoUnit.HOURS,
    @JsonProperty
    val workAlone: Boolean = false,
    @JsonProperty
    val sessionAliases: Set<String> = emptySet(),
    @JsonProperty
    val sessionAliasesPattern: String? = null
)