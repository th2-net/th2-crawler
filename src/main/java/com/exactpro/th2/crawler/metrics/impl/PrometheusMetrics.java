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

package com.exactpro.th2.crawler.metrics.impl;

import static com.exactpro.th2.common.metrics.CommonMetrics.*;

import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.dataprovider.grpc.EventResponse;
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse;

import io.prometheus.client.Counter;
import io.prometheus.client.Counter.Child;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Timer;
import org.jetbrains.annotations.NotNull;

public class PrometheusMetrics implements CrawlerMetrics {
    private static final Histogram PROCESSING_TIME = Histogram.build()
            .name("th2_crawler_processing_data_time_seconds")
            .help("time in seconds to process an interval")
            .buckets(0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 7.5, 10, 15, 20, 25, 30, 45, 60, 90, 120)
            .labelNames("data_type", "method")
            .register();

    private static final Counter PROCESSED_DATA_COUNT = Counter.build()
            .name("th2_crawler_processed_data_count")
            .help("number of data processed by the crawler")
            .labelNames("data_type")
            .register();

    @SuppressWarnings("StaticCollection")
    private static final Map<DataType, Child> PROCESSED_DATA_COUNT_MAP = createMetricMap(PROCESSED_DATA_COUNT, DataType.class);

    //region Message's metrics
    private static final Gauge LAST_MESSAGE_SEQUENCE = Gauge.build()
            .name("th2_crawler_processing_message_sequence_number")
            .help("contains the sequence number of the last processed message for corresponding alias and direction")
            .labelNames(SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .register();
    private static final Gauge LAST_MESSAGE_TIMESTAMP = Gauge.build()
            .name("th2_crawler_processing_message_timestamp_milliseconds")
            .help("contains the timestamp of the last processed message in milliseconds for corresponding alias and direction")
            .labelNames(SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .register();
    //endregion
    private static final Gauge LAST_EVENT_TIMESTAMP = Gauge.build()
            .name("th2_crawler_processing_event_timestamp_milliseconds")
            .help("contains the timestamp (creation time) of the last processed event in milliseconds")
            .register();

    private static final Gauge LAST_INTERVAL_TIMESTAMP = Gauge.build()
            .name("th2_crawler_processing_start_time_interval_milliseconds")
            .help("contains the timestamp (creation time) of the last processed interval in milliseconds")
            .register();

    //region Invocations metrics
    private static final Counter DATA_PROVIDER_INVOCATIONS = Counter.build()
            .name("th2_crawler_data_provider_api_calls_count")
            .help("total number of invocations of corresponding data provider's method")
            .labelNames("method")
            .register();

    private static final Counter DATA_PROCESSOR_INVOCATIONS = Counter.build()
            .name("th2_crawler_processor_api_calls_number")
            .help("total number of invocations of corresponding data processor's method")
            .labelNames("method")
            .register();
    //endregion

    @SuppressWarnings("StaticCollection")
    private static final Map<ProcessorMethod, Child> DATA_PROCESSOR_INVOCATIONS_MAP = createMetricMap(DATA_PROCESSOR_INVOCATIONS, ProcessorMethod.class);

    public static final Counter INCOMING_MESSAGE_COUNTER = Counter.build()
            .name("th2_crawler_incoming_data_items_count")
            .help("number of data items received from data provider")
            .register();

    @NotNull
    private static <T extends Enum<T>> Map<T, Child> createMetricMap(Counter counter, Class<T> keyType) {
        Map<T, Child> map = new EnumMap<>(keyType);
        for (T dataType : EnumSet.allOf(keyType)) {
            map.put(dataType, counter.labels(dataType.name()));
        }
        return map;
    }

    @Override
    public void lastMessage(String alias, Direction direction, MessageGroupResponse messageData) {
        String[] labels = {alias, direction.name()};
        LAST_MESSAGE_SEQUENCE
                .labels(labels)
                .set(messageData.getMessageId().getSequence());
        LAST_MESSAGE_TIMESTAMP
                .labels(labels)
                .set(CrawlerUtils.fromTimestamp(messageData.getTimestamp()).toEpochMilli());
    }

    @Override
    public void currentInterval(Interval interval) {
        LAST_INTERVAL_TIMESTAMP.set(interval.getEndTime().toEpochMilli());
    }

    @Override
    public void lastEvent(EventResponse event) {
        LAST_EVENT_TIMESTAMP.set(CrawlerUtils.fromTimestamp(event.getStartTimestamp()).toEpochMilli());
    }

    @Override
    public void processorMethodInvoked(ProcessorMethod method) {
        DATA_PROCESSOR_INVOCATIONS_MAP.get(method).inc();
    }

    @Override
    public void providerMethodInvoked(ProviderMethod method) {
        DATA_PROVIDER_INVOCATIONS.labels(method.name()).inc();
    }

    @Override
    public <T> T measureTime(DataType dataType, Method method, CrawlerDataOperation<T> function) {
        Timer timer = PROCESSING_TIME.labels(dataType.getTypeName(), method.name()).startTimer(); //FIXME: pass histogram child
        try {
            return function.call();
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public <T> T measureTimeWithException(DataType dataType, Method method, CrawlerDataOperationWithException<T> function) throws IOException, UnexpectedDataProcessorException {
        Timer timer = PROCESSING_TIME.labels(dataType.getTypeName(), method.name()).startTimer(); //FIXME: pass histogram child
        try {
            return function.call();
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public void updateProcessedData(DataType dataType, long count) {
        PROCESSED_DATA_COUNT_MAP.get(dataType).inc(count);
    }
}