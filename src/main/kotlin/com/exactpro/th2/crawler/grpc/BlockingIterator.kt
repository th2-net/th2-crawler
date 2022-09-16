/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.crawler.grpc

import java.util.concurrent.locks.ReentrantLock

class BlockingIterator<T> : Iterator<T> {
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val queue = ArrayDeque<T>()
    private var completeFlag = false
    @Volatile
    private var error: Throwable? = null

    fun complete(t: Throwable? = null) {
        try {
            lock.lock()
            error = t
            completeFlag = true
            condition.signalAll()
        } finally {
            lock.unlock()
        }
    }

    fun put(value: T) {
        try {
            lock.lock()
            require(!completeFlag) {
                "Iterator has already completed"
            }

            queue.addLast(value)
            condition.signalAll()
        } finally {
            lock.unlock()
        }
    }

    override fun hasNext(): Boolean {
        try {
            lock.lock()
            return if(queue.isNotEmpty()) {
                true
            } else {
                if (completeFlag) {
                    error?.let {
                        throw it
                    }
                    false
                } else {
                    condition.await()
                    hasNext()
                }
            }
        } finally {
            lock.unlock()
        }
    }

    override fun next(): T {
        try {
            lock.lock()
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            return queue.removeFirst()
        } finally {
            lock.unlock()
        }
    }
}