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

import java.io.IOException;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Timer;

import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_DIRECTION_LABEL_NAME;
import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_SESSION_ALIAS_LABEL_NAME;

public class PrometheusMetrics implements CrawlerMetrics {
    private final Histogram processingTime = Histogram.build()
            .name("th2_crawler_processing_data_time_seconds")
            .help("time in seconds to process an interval")
            .buckets(0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 7.5, 10, 15, 20, 25, 30, 45, 60)
            .labelNames("data_type", "method")
            .register();
    private final Counter processedDataCount = Counter.build()
            .name("th2_crawler_processed_data_count")
            .help("number of data processed by the crawler")
            .labelNames("data_type")
            .register();
    //region Message's metrics
    private final Gauge lastMessageSequence = Gauge.build()
            .name("th2_crawler_processing_message_sequence_number")
            .help("contains the sequence number of the last processed message for corresponding alias and direction")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .register();
    private final Gauge lastMessageTimestamp = Gauge.build()
            .name("th2_crawler_processing_message_timestamp_milliseconds")
            .help("contains the timestamp of the last processed message in milliseconds for corresponding alias and direction")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .register();
    //endregion
    private final Gauge lastEventTimestamp = Gauge.build()
            .name("th2_crawler_processing_event_timestamp_milliseconds")
            .help("contains the timestamp (creation time) of the last processed event in milliseconds")
            .register();

    private final Gauge lastIntervalTimestamp = Gauge.build()
            .name("th2_crawler_processing_start_time_interval_milliseconds")
            .help("contains the timestamp (creation time) of the last processed interval in milliseconds")
            .register();

    //region Invocations metrics
    private final Counter dataProviderInvocations = Counter.build()
            .name("th2_crawler_data_provider_api_calls_count")
            .help("total number of invocations of corresponding data provider's method")
            .labelNames("method")
            .register();

    private final Counter dataProcessorInvocations = Counter.build()
            .name("th2_crawler_processor_api_calls_number")
            .help("total number of invocations of corresponding data processor's method")
            .labelNames("method")
            .register();
    //endregion

    @Override
    public void lastMessage(String alias, Direction direction, MessageData messageData) {
        String[] labels = {alias, direction.name()};
        lastMessageSequence
                .labels(labels)
                .set(messageData.getMessageId().getSequence());
        lastMessageTimestamp
                .labels(labels)
                .set(CrawlerUtils.fromTimestamp(messageData.getTimestamp()).toEpochMilli());
    }

    @Override
    public void currentInterval(Interval interval) {
        lastIntervalTimestamp.set(interval.getEndTime().toEpochMilli());
    }

    @Override
    public void lastEvent(EventData event) {
        lastEventTimestamp.set(CrawlerUtils.fromTimestamp(event.getStartTimestamp()).toEpochMilli());
    }

    @Override
    public void processorMethodInvoked(ProcessorMethod method) {
        dataProcessorInvocations.labels(method.name()).inc();
    }

    @Override
    public void providerMethodInvoked(ProviderMethod method) {
        dataProviderInvocations.labels(method.name()).inc();
    }

    @Override
    public <T> T measureTime(DataType dataType, Method method, CrawlerDataOperation<T> function) {
        Timer timer = processingTime.labels(dataType.getTypeName(), method.name()).startTimer();
        try {
            return function.call();
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public <T> T measureTimeWithException(DataType dataType, Method method, CrawlerDataOperationWithException<T> function) throws IOException, UnexpectedDataProcessorException {
        Timer timer = processingTime.labels(dataType.getTypeName(), method.name()).startTimer();
        try {
            return function.call();
        } finally {
            timer.observeDuration();
        }
    }

    @Override
    public void updateProcessedData(DataType dataType, long count) {
        processedDataCount.labels(dataType.getTypeName()).inc(count);
    }
}