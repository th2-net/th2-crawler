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
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handshaker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);
    private final DataProcessorService dataProcessor;

    public Handshaker(DataProcessorService dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    public SendingReport handshake(CrawlerId crawlerId,
                                   Interval interval,
                                   DataProcessorInfo dataProcessorInfo,
                                   long numberOfEvents,
                                   long numberOfMessages) {
        LOGGER.debug("Performing handshake...");
        DataProcessorInfo info = dataProcessor.crawlerConnect(CrawlerInfo.newBuilder().setId(crawlerId).build());

        String dataProcessorName = info.getName();
        String dataProcessorVersion = info.getVersion();

        CrawlerAction action;
        if (info.equals(dataProcessorInfo)) {
            LOGGER.info("Got the same name ({}) and version ({}) from repeated crawlerConnect", dataProcessorName, dataProcessorVersion);
            action = CrawlerAction.CONTINUE;
        } else {
            LOGGER.info("Got another name ({}) or version ({}) from repeated crawlerConnect, restarting component", dataProcessorName, dataProcessorVersion);
            action = CrawlerAction.STOP;
        }
        return new SendingReport(action, interval, dataProcessorName, dataProcessorVersion, numberOfEvents, numberOfMessages);
    }
}
