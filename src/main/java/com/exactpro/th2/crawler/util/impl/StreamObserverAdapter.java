/*
 *  Copyright 2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.util.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public abstract class StreamObserverAdapter<T, R> implements StreamObserver<T> {
    protected final CompletableFuture<R> future = new CompletableFuture<>();

    public final CompletableFuture<R> getFuture() {
        return future;
    }

    public static <T> StreamObserverAdapter<T, T> singleValueAdapter() {
        return new SingleResponseAdapter<>();
    }

    public static <T> StreamObserverAdapter<T, List<T>> multipleValuesAdapter() {
        return new MultipleResponseAdapter<>();
    }

    private static class SingleResponseAdapter<T> extends StreamObserverAdapter<T, T> {
        private static final Logger LOGGER = LoggerFactory.getLogger(SingleResponseAdapter.class);
        private T stored;

        @Override
        public void onNext(T value) {
            if (stored != null) {
                LOGGER.warn("Cannot store value. It was already set");
                return;
            }
            stored = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (future.isDone()) {
                LOGGER.warn("The future is already complete. Cannot complete it again");
                return;
            }
            T storedValue = stored;
            if (storedValue == null) {
                future.completeExceptionally(new IllegalStateException("The steam is completed before any value is received"));
            } else {
                future.complete(storedValue);
            }
        }
    }

    private static class MultipleResponseAdapter<T> extends StreamObserverAdapter<T, List<T>> {
        private static final Logger LOGGER = LoggerFactory.getLogger(MultipleResponseAdapter.class);
        private final List<T> data = new ArrayList<>();

        @Override
        public void onNext(T value) {
            data.add(value);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (future.isDone()) {
                LOGGER.warn("The future is already complete. Cannot complete it again");
                return;
            }
            future.complete(data);
        }
    }
}
