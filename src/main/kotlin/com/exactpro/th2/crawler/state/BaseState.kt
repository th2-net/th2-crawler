/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.crawler.state

import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.fasterxml.jackson.annotation.JsonTypeInfo


/**
 * This interface is used to mark the recovery state class and choose the right one during deserialization
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "version",
    include = JsonTypeInfo.As.PROPERTY,
)
interface BaseState

/**
 * The version of the recovery state that can be used to determinate the conversions that should be applied to get the current state version
 */
interface VersionMarker {
    val name: String
    val number: Int

    operator fun compareTo(other: VersionMarker): Int = this.number.compareTo(other.number)
}

/**
 * The state versions. When new version is added it should be last one in the enum list.
 *
 * The order is important. It will be used to determinate which conversions should be made
 */
enum class Version : VersionMarker {
    V_1,
    V_2;

    override val number: Int
        get() = ordinal
}

interface StateProvider {
    val version: VersionMarker
    val stateClass: Class<out BaseState>

    /**
     * Returns the converter from the current state to the next one in versions hierarchy.
     *
     * If the state provided by the current provider is a terminal state (the current one) return `null`.
     *
     * If the state is not terminal but the conversion to the next state is not supported use [StateService.unsupportedMigrationTo] method to create a converter
     */
    val converter: StateConverter<BaseState, BaseState>?
}

interface StateConverter<IN, out OUT> {
    val target: VersionMarker
    fun convert(input: IN, dataProvider: DataProviderService): OUT
}