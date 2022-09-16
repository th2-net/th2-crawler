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

import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.util.CrawlerTime;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

public class CrawlerContext {

    private final CrawlerTime crawlerTime;

    private final CrawlerMetrics metrics;

    private final CrawlerConfiguration configuration;

    public CrawlerContext(CrawlerTime crawlerTime, CrawlerMetrics metrics, CrawlerConfiguration configuration) {
        this.crawlerTime = requireNonNull(crawlerTime, "'Crawler time' parameter");
        this.metrics = requireNonNull(metrics, "'Metrics' parameter");
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
    }

    @NotNull
    public CrawlerTime getCrawlerTime() {
        return crawlerTime;
    }


    @NotNull
    public CrawlerMetrics getMetrics() {
        return metrics;
    }

    @NotNull
    public CrawlerConfiguration getConfiguration() {
        return configuration;
    }
}
