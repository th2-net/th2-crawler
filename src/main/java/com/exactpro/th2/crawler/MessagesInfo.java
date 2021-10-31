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

package com.exactpro.th2.crawler;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.state.v1.StreamKey;

public class MessagesInfo {
    private final Interval interval;
    private final DataProcessorInfo dataProcessorInfo;
    private final Map<StreamKey, MessageID> startIds;
    private final Collection<String> aliases;
    private final Instant from;
    private final Instant to;

    public MessagesInfo(Interval interval,
                         DataProcessorInfo dataProcessorInfo,
                         Map<StreamKey, MessageID> startIds,
                         Collection<String> aliases,
                         Instant from, Instant to) {
        this.interval = interval;
        this.dataProcessorInfo = dataProcessorInfo;
        this.startIds = startIds;
        this.aliases = aliases;
        this.from = from;
        this.to = to;
    }

    public Interval getInterval() {
        return interval;
    }

    public DataProcessorInfo getDataProcessorInfo() {
        return dataProcessorInfo;
    }

    public Map<StreamKey, MessageID> getStartIds() {
        return startIds;
    }

    public Collection<String> getAliases() {
        return aliases;
    }

    public Instant getFrom() {
        return from;
    }

    public Instant getTo() {
        return to;
    }
}
