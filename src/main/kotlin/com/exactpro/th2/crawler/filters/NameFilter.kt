/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.filters

data class NameFilter(
    val names: Set<String> = emptySet(),
    val mode: Mode = Mode.DROP,
) {
    enum class Mode { ACCEPT, DROP }

    fun accept(name: String): Boolean = when(mode) {
        Mode.ACCEPT -> names.contains(name)
        Mode.DROP -> !names.contains(name)
    }
}
