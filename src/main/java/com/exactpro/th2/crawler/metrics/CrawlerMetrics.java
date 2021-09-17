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

package com.exactpro.th2.crawler.metrics;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.MessageData;

public interface CrawlerMetrics {

    void lastMessage(String alias, Direction direction, MessageData messageData);

    void currentInterval(Interval interval);

    void lastEvent(EventData event);

    void processorMethodInvoked(ProcessorMethod method);

    void providerMethodInvoked(ProviderMethod method);

    <T> T measureTime(DataType dataType, CrawlerDataOperation<T> function) throws IOException;

    enum ProcessorMethod { CRAWLER_CONNECT, SEND_EVENT, SEND_MESSAGE }

    enum ProviderMethod { SEARCH_MESSAGES, SEARCH_EVENTS }


    /**
     * This interface should be used to pass the crawler call that processes
     * @param <T>
     */
    @FunctionalInterface
    interface CrawlerDataOperation<T> {
        T call() throws IOException;
    }
}
