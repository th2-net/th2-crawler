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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito

class TestStateService {
    enum class TestVersion : VersionMarker {
        A, B, C;
        override val number: Int = ordinal
    }

    class TestStateProvider(
        override val version: VersionMarker,
        override val stateClass: Class<out BaseState>,
        override val converter: StateConverter<BaseState, BaseState>? = null,
    ) : StateProvider

    class StateA(
        val content: String
    ) : BaseState

    class StateB(
        val data: Int
    ) : BaseState

    class StateC(
        val value: Int
    ) : BaseState

    private val dataProvider: DataProviderService = Mockito.mock(DataProviderService::class.java)

    @Nested
    inner class CorrectConfiguration {
        private val stateService = StateService.create<StateC>(
            listOf(
                TestStateProvider(
                    TestVersion.A,
                    StateA::class.java,
                    StdStateConverter.create<StateA, StateB>(TestVersion.B) { input, _ ->
                        StateB(input.content.toInt())
                    }),
                TestStateProvider(
                    TestVersion.B,
                    StateB::class.java,
                    StdStateConverter.create<StateB, StateC>(TestVersion.C) { input, _ ->
                        StateC(input.data)
                    }),
                TestStateProvider(TestVersion.C, StateC::class.java),
            ).associateBy { it.version },
            dataProvider,
            defaultImplementation = StateA::class.java
        )

        @Test
        fun `correctly deserialize current state`() {
            val stateC = stateService.deserialize("""{ "version": "C", "value": 5 }""")
            Assertions.assertNotNull(stateC)
            stateC!!
            assertEquals(5, stateC.value)
        }

        @Test
        fun `correctly deserialize previous state`() {
            val stateC = stateService.deserialize("""{ "version": "B", "data": 4 }""")
            Assertions.assertNotNull(stateC)
            stateC!!
            assertEquals(4, stateC.value)
        }


        @Test
        fun `correctly deserialize old state`() {
            val stateC = stateService.deserialize("""{ "version": "A", "content": "42" }""")
            Assertions.assertNotNull(stateC)
            stateC!!
            assertEquals(42, stateC.value)
        }

        @Test
        fun `uses default implementation if cannot deserialize by the version ID`() {
            val stateC = stateService.deserialize("""{ "content": "42" }""")
            Assertions.assertNotNull(stateC)
            stateC!!
            assertEquals(42, stateC.value)
        }

        @Test
        fun `fails if cannot deserialize using default implementation`() {
            Assertions.assertThrows(MissingKotlinParameterException::class.java) {
                stateService.deserialize("""{ "data": "42" }""")
            }.also {
                val messageToCheck =
                    "failed for JSON property content due to missing (therefore NULL) value for creator parameter content which is a non-nullable type"
                Assertions.assertTrue(it.message?.contains(messageToCheck) ?: false) {
                    "Does not contain message: '$messageToCheck'. The actual exception is $it"
                }
            }
        }

        @Test
        fun `correctly serializes all versions`() {
            val serialized = stateService.serialize(StateC(42))
            val jsonNode = ObjectMapper().readTree(serialized)
            assertEquals("C", jsonNode.get("version").asText()) {
                "Unexpected result: $serialized"
            }
        }
    }

    enum class DuplicatedMarkers : VersionMarker {
        V1, V2;

        override val number: Int
            get() = 1
    }

    @Nested
    inner class IncorrectConfiguration {
        @Test
        fun `duplicated versions`() {
            assertThrows<IllegalArgumentException> {
                StateService.create<StateC>(
                    listOf(
                        TestStateProvider(
                            DuplicatedMarkers.V1,
                            StateA::class.java,
                            StdStateConverter.create<StateA, StateB>(DuplicatedMarkers.V2) { input, _ ->
                                StateB(input.content.toInt())
                            }),
                        TestStateProvider(
                            DuplicatedMarkers.V2,
                            StateB::class.java,
                            StdStateConverter.create<StateB, StateC>(DuplicatedMarkers.V2) { input, _ ->
                                StateC(input.data)
                            }),
                        TestStateProvider(DuplicatedMarkers.V2, StateC::class.java),
                    ).associateBy { it.version },
                    dataProvider,
                    defaultImplementation = StateA::class.java
                )
            }.also {
                assertEquals("Some versions have duplicated numbers: V1=1, V2=1", it.message)
            }
        }

        @Test
        fun `missing converter`() {
            assertThrows<IllegalArgumentException> {
                StateService.create<StateC>(
                    listOf(
                        TestStateProvider(
                            TestVersion.A,
                            StateA::class.java,
                            StdStateConverter.create<StateA, StateB>(TestVersion.B) { input, _ ->
                                StateB(input.content.toInt())
                            }),
                        TestStateProvider(TestVersion.B, StateB::class.java),
                        TestStateProvider(TestVersion.C, StateC::class.java),
                    ).associateBy { it.version },
                    dataProvider,
                    defaultImplementation = StateA::class.java
                )
            }.also {
                assertEquals("No all versions have expected converters. Missing converters to versions: [C]", it.message)
            }
        }

        @Test
        fun `reordered converters`() {
            assertThrows<IllegalArgumentException> {
                StateService.create<StateC>(
                    listOf(
                        TestStateProvider(
                            TestVersion.A,
                            StateA::class.java,
                            StdStateConverter.create<StateA, StateB>(TestVersion.C) { input, _ ->
                                StateB(input.content.toInt())
                            }),
                        TestStateProvider(
                            TestVersion.B,
                            StateB::class.java,
                            StdStateConverter.create<StateB, StateC>(TestVersion.B) { input, _ ->
                                StateC(input.data)
                            }),
                        TestStateProvider(TestVersion.C, StateC::class.java),
                    ).associateBy { it.version },
                    dataProvider,
                    defaultImplementation = StateA::class.java
                )
            }.also {
                assertEquals("Unordered converters. Expected order: [B, C] but actual order: [C, B]", it.message)
            }
        }

        @Test
        fun `duplicated state classes`() {
            assertThrows<IllegalArgumentException> {
                StateService.create<StateC>(
                    listOf(
                        TestStateProvider(
                            TestVersion.A,
                            StateA::class.java,
                            StdStateConverter.create<StateA, StateB>(TestVersion.B) { input, _ ->
                                StateB(input.content.toInt())
                            }),
                        TestStateProvider(
                            TestVersion.B,
                            StateA::class.java,
                            StdStateConverter.create<StateB, StateC>(TestVersion.C) { input, _ ->
                                StateC(input.data)
                            }),
                        TestStateProvider(TestVersion.C, StateC::class.java),
                    ).associateBy { it.version },
                    dataProvider,
                    defaultImplementation = StateA::class.java
                )
            }.also {
                assertEquals(
                    "Duplicated versions for state class com.exactpro.th2.crawler.state.TestStateService\$StateA: A, B",
                    it.message
                )
            }
        }
    }
}