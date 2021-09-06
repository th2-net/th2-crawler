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

import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import mu.KotlinLogging
import org.jetbrains.annotations.Contract
import java.util.ServiceLoader

class StateService<CUR : BaseState>(
    private val currentStateClass: Class<out CUR>,
    private val providers: Map<VersionMarker, StateProvider>,
    private val dataProvide: DataProviderService,
    private val defaultImplementation: Class<out BaseState>? = null
) {
    private val classToVersion: Map<Class<out BaseState>, VersionMarker> =
        providers.entries.map { (version, provider) -> provider.stateClass to version }
            .groupingBy { it.first }
            .aggregate { key, current, element, first ->
                val version = element.second
                require(first) { "Duplicated versions for state $key: $current, $version" }
                version
            }
    private val mapper = JsonMapper().registerKotlinModule()
        .registerModule(JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .apply {
            registerSubtypes(*providers.map { it.run { NamedType(value.stateClass, key.name) } }.toTypedArray())
            defaultImplementation?.also { addHandler(DeserializationProblemHandlerImpl(it)) }
        }
    private val versions = providers.keys.sortedBy(VersionMarker::number)
    private val currentVersion = versions.last()

    init {
        val version = requireNotNull(classToVersion[currentStateClass]) {
            "Current state ${currentStateClass.canonicalName} does not have a provider"
        }
        require(currentVersion == version) {
            "$currentVersion does not match version $version for ${currentStateClass.canonicalName}"
        }
        defaultImplementation?.also {
            requireNotNull(classToVersion[it]) {
                "The default implementation ${it.canonicalName} does not have a provider"
            }
        }
        require(versions.size == versions.map(VersionMarker::number).toSet().size) {
            "Some versions have duplicated numbers: ${versions.joinToString(", ") { "${it.name}=${it.number}" } }"
        }
        val names = versions.map(VersionMarker::name).toSet()
        require(versions.size == names.size) {
            "Some versions have duplicated names: $names"
        }
        LOGGER.info { "StateService initialized with following versions loaded: ${providers.keys}. Current version: $currentVersion" }
    }

    fun serialize(state: CUR): String {
        return mapper.writeValueAsString(state)
    }

    @Contract("null -> null; !null -> !null")
    fun deserialize(data: String?): CUR? {
        if (data == null) {
            return null
        }
        val baseState = mapper.readValue<BaseState>(data)
        val stateClass = baseState::class.java
        val deserializedVersion = checkNotNull(classToVersion[stateClass]) {
            "Unknown state class: ${stateClass.canonicalName}"
        }
        check(deserializedVersion <= currentVersion) { "Current version $currentVersion is lower than deserialized $deserializedVersion" }
        if (deserializedVersion == currentVersion) {
            return currentStateClass.cast(baseState)
        }

        val versionsGap = versions.dropWhile { it.number != deserializedVersion.number }
        return versionsGap.fold(baseState) { state, version ->
            if (version.number == currentVersion.number) { // skip conversion
                return@fold state
            }
            val provider = checkNotNull(providers[version]) {
                "Cannot find provider for version $version"
            }
            LOGGER.trace { "Converting state from $version to the next one" }
            try {
                provider.converter?.convert(state, dataProvide)
                    ?: error("provider for version $version does not have converter")
            } catch (ex: Exception) {
                throw IllegalStateException("cannot convert ${state.className} to the next version after $version", ex)
            }
        }.let(currentStateClass::cast)
    }

    private class DeserializationProblemHandlerImpl(
        private val defaultImplementation: Class<out BaseState>
    ): DeserializationProblemHandler() {
        override fun handleMissingTypeId(
            ctxt: DeserializationContext,
            baseType: JavaType,
            idResolver: TypeIdResolver,
            failureMsg: String
        ): JavaType? {
            return when {
                baseType.isTypeOrSubTypeOf(BaseState::class.java) -> {
                    LOGGER.info { "Using default implementation $defaultImplementation to deserialize state" }
                    ctxt.typeFactory.constructSimpleType(defaultImplementation, emptyArray())
                }
                else -> null
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        @JvmStatic
        inline fun <reified T : BaseState> create(
            providers: Map<VersionMarker, StateProvider>,
            dataProvider: DataProviderService,
            defaultImplementation: Class<out BaseState>? = null,
        ): StateService<T> = create(T::class.java, providers, dataProvider, defaultImplementation)

        @JvmStatic
        fun <T : BaseState> create(
            currentStateClass: Class<out T>,
            providers: Map<VersionMarker, StateProvider>,
            dataProvider: DataProviderService,
            defaultImplementation: Class<out BaseState>? = null,
        ) = StateService(currentStateClass, providers, dataProvider, defaultImplementation)

        @JvmStatic
        inline fun <reified T : BaseState> createFromClasspath(
            dataProvider: DataProviderService,
            defaultImplementation: Class<out BaseState>? = null,
        ): StateService<T> = createFromClasspath(T::class.java, dataProvider, defaultImplementation)

        /**
         * Loads state providers using [ServiceLoader] from the classpath
         */
        @JvmStatic
        fun <T : BaseState> createFromClasspath(
            currentStateClass: Class<out T>,
            dataProvider: DataProviderService,
            defaultImplementation: Class<out BaseState>? = null,
        ): StateService<T> =
            create(
                currentStateClass,
                ServiceLoader.load(StateProvider::class.java)
                    .groupingBy(StateProvider::version)
                    .aggregate { key, current, element, first ->
                        require(first) { "Duplicated providers for version $key: ${current.className}, ${element.className}" }
                        element
                    },
                dataProvider,
                defaultImplementation,
            )

        private val Any?.className: String?
            get() = this?.run { this::class.java.canonicalName }
    }
}
