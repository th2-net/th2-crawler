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

package com.exactpro.th2.crawler;

import java.util.List;
import java.util.Objects;

import com.exactpro.th2.crawler.metrics.CrawlerMetrics;

public abstract class AbstractStrategy<T extends CrawlerData<C>, C extends Continuation> implements DataTypeStrategy<T, C> {
    protected final CrawlerMetrics metrics;

    public AbstractStrategy(CrawlerMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "'Metrics' parameter");
    }

    public abstract static class AbstractCrawlerData<C extends Continuation, DATA> implements CrawlerData<C> {
        private final List<DATA> data;
        private final boolean needsNextRequest;

        protected AbstractCrawlerData(List<DATA> data, boolean needsNextRequest) {
            this.data = Objects.requireNonNull(data, "'Data' parameter");
            this.needsNextRequest = needsNextRequest;
        }

        public final List<DATA> getData() {
            return data;
        }

        @Override
        public final boolean getHasData() {
            return !data.isEmpty();
        }

        @Override
        public final boolean isNeedsNextRequest() {
            return needsNextRequest;
        }

        @Override
        public int size() {
            return data.size();
        }
    }
}
