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

abstract class StdStateConverter<IN : BaseState, OUT>(
    private val inputClass: Class<IN>
) : StateConverter<BaseState, OUT> {
    final override fun convert(input: BaseState, dataProvider: DataProviderService): OUT {
        return convertState(inputClass.cast(input), dataProvider)
    }

    abstract fun convertState(input: IN, dataProvider: DataProviderService): OUT

    companion object {
        @JvmStatic
        inline fun <reified IN : BaseState, OUT> create(noinline action: (IN, DataProviderService) -> OUT): StateConverter<BaseState, OUT> =
            create(IN::class.java, action)

        @JvmStatic
        fun <IN : BaseState, OUT> create(input: Class<IN>, action: (IN, DataProviderService) -> OUT): StateConverter<BaseState, OUT> =
            object : StdStateConverter<IN, OUT>(input) {
                override fun convertState(input: IN, dataProvider: DataProviderService): OUT = action(input, dataProvider)
            }
    }

}