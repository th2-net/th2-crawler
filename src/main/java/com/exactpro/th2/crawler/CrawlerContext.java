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

package com.exactpro.th2.crawler;

import java.util.Objects;

import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.impl.PrometheusMetrics;
import com.exactpro.th2.crawler.util.CrawlerTime;
import com.exactpro.th2.crawler.util.impl.CrawlerTimeImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CrawlerContext {

    private CrawlerTime crawlerTime;

    private CrawlerMetrics metrics;

    @Nullable
    public CrawlerTime getCrawlerTime() {
        return crawlerTime;
    }

    public CrawlerContext setCrawlerTime(CrawlerTime crawlerTime) {
        this.crawlerTime = Objects.requireNonNull(crawlerTime, "'Crawler time' parameter");
        return this;
    }

    @Nullable
    public CrawlerMetrics getMetrics() {
        return metrics;
    }

    public CrawlerContext setMetrics(CrawlerMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "'Metrics' parameter");
        return this;
    }
}
