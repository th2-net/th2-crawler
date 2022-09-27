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

import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.util.concurrent.CountDownLatch

class ClientObserver<T>(
    private val sampler: Sampler,
    private val step: Int,
) : StreamObserver<T> {
    private val done = CountDownLatch(1)

    override fun onNext(value: T) {
        LOGGER.debug { "onNext has been called $value" }
        sampler.inc(step)
    }

    override fun onError(t: Throwable) {
        LOGGER.error (t) { "onError has been called" }
        done.countDown()
    }

    override fun onCompleted() {
        LOGGER.info { "onCompleted has been called" }
        done.countDown()
    }

    fun await() {
        done.await()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}