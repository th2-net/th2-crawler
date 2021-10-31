/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.EventID;
import com.google.protobuf.Timestamp;

public class EventsSearchParameters {
    private final Timestamp from;
    private final Timestamp to;
    private final int batchSize;
    private final EventID resumeId;

    public EventsSearchParameters(Timestamp from, Timestamp to, int batchSize, EventID resumeId) {
        this.from = Objects.requireNonNull(from, "Timestamp 'from' must not be null");
        this.to = Objects.requireNonNull(to, "Timestamp 'to' must not be null");
        this.batchSize = batchSize;
        this.resumeId = resumeId;
    }

    public Timestamp getFrom() {
        return from;
    }

    public Timestamp getTo() {
        return to;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public EventID getResumeId() {
        return resumeId;
    }
}
