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

package com.exactpro.th2.crawler.util;

import java.util.function.Function;
import java.util.function.Supplier;

import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import io.grpc.stub.StreamObserver;

public class TestUtil {
    private TestUtil() {
    }

    public static Stubber doObserverError(Supplier<Exception> supplier) {
        return Mockito.doAnswer(invk -> {
            StreamObserver<?> observer = invk.getArgument(1);
            observer.onError(supplier.get());
            return null;
        });
    }

    public static <T> Stubber doObserverAnswer(T... values) {
        return Mockito.doAnswer(invk -> {
            StreamObserver<T> observer = invk.getArgument(1);
            for (T value : values) {
                observer.onNext(value);
            }
            observer.onCompleted();
            return null;
        });
    }

    public static <T, R> Stubber doObserverAnswer(Function<T, R> answer) {
        return Mockito.doAnswer(invk -> {
            T params = invk.getArgument(0);
            StreamObserver<R> observer = invk.getArgument(1);
            observer.onNext(answer.apply(params));
            observer.onCompleted();
            return null;
        });
    }
}
