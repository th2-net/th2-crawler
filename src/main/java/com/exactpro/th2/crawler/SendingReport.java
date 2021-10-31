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

import com.exactpro.cradle.intervals.Interval;

public class SendingReport {
    private final CrawlerAction action;
    private final String newName;
    private final String newVersion;
    private final long numberOfEvents;
    private final long numberOfMessages;
    private final Interval interval;

    public SendingReport(CrawlerAction action, Interval interval, String newName, String newVersion, long numberOfEvents, long numberOfMessages) {
        this.action = action;
        this.interval = interval;
        this.newName = newName;
        this.newVersion = newVersion;
        this.numberOfEvents = numberOfEvents;
        this.numberOfMessages = numberOfMessages;
    }

    public CrawlerAction getAction() {
        return action;
    }

    public String getNewName() {
        return newName;
    }

    public String getNewVersion() {
        return newVersion;
    }

    public long getNumberOfEvents() {
        return numberOfEvents;
    }

    public long getNumberOfMessages() {
        return numberOfMessages;
    }

    public Interval getInterval() {
        return interval;
    }
}
