/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class CrawlerTimeTestImpl implements CrawlerTime {
    private Instant instant = Instant.parse("2021-07-11T18:00:00.00Z");
    private long count = -1;

    @Override
    public Instant now() {
        count++;
        return instant.plus(10 * count, ChronoUnit.SECONDS);
    }
}
