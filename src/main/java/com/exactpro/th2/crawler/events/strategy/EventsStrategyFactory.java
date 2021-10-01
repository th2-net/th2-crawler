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

package com.exactpro.th2.crawler.events.strategy;

import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.DataTypeStrategy;
import com.exactpro.th2.crawler.DataTypeStrategyFactory;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.ResumeId;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.google.auto.service.AutoService;

@AutoService(DataTypeStrategyFactory.class)
public class EventsStrategyFactory implements DataTypeStrategyFactory<EventsCrawlerData, ResumeId> {
    @NotNull
    @Override
    public DataType getDataType() {
        return DataType.EVENTS;
    }

    @NotNull
    @Override
    public DataTypeStrategy<EventsCrawlerData, ResumeId> create(
            @NotNull IntervalsWorker worker,
            @NotNull DataProviderService provider,
            @NotNull StateService<RecoveryState> stateService,
            @NotNull CrawlerMetrics metrics,
            @NotNull CrawlerConfiguration config) {
        return new EventsStrategy(provider, metrics, config);
    }
}
