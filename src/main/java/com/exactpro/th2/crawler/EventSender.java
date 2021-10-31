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

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventSender.class);
    private final CrawlerConfiguration configuration;
    private final int batchSize;
    private final CrawlerId crawlerId;
    private final DataProcessorService dataProcessor;
    private final DataProviderService dataProviderService;
    private final IntervalsWorker intervalsWorker;
    private final StateService<RecoveryState> stateService;
    private final Handshaker handshaker;
    private final DataRequester dataRequester;

    public EventSender(@NotNull CrawlerConfiguration configuration,
                         @NotNull DataProcessorService dataProcessor,
                         @NotNull DataProviderService dataProviderService,
                         @NotNull IntervalsWorker intervalsWorker,
                         @NotNull StateService<RecoveryState> stateService,
                         @NotNull Handshaker handshaker
    ) {
        this.configuration = configuration;
        this.dataProcessor = dataProcessor;
        this.dataProviderService = dataProviderService;
        this.intervalsWorker = intervalsWorker;
        this.stateService = stateService;
        this.handshaker = handshaker;
        batchSize = configuration.getBatchSize();
        crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        dataRequester = new DataRequester(dataProviderService);
    }

    public SendingReport sendEvents(EventsInfo info) throws IOException {
        EventResponse response;
        Interval interval = info.getInterval();
        EventID resumeId = info.getStartId();
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(info.getFrom());
        Timestamp toTimestamp = MessageUtils.toTimestamp(info.getTo());
        long numberOfEvents = 0L;

        long diff = 0L;

        String dataProcessorName = info.getDataProcessorInfo().getName();
        String dataProcessorVersion = info.getDataProcessorInfo().getVersion();

        while (search) {

            EventDataRequest.Builder eventRequestBuilder = EventDataRequest.newBuilder();

            SearchResult<EventData> result = dataRequester.searchEvents(
                    new EventsSearchParameters(fromTimestamp, toTimestamp, batchSize, resumeId));
            List<EventData> events = result.getData();

            if (events.isEmpty()) {
                LOGGER.info("No more events in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            EventData lastEvent = events.get(events.size() - 1);

            resumeId = lastEvent.getEventId();

            EventDataRequest eventRequest = eventRequestBuilder.setId(crawlerId).addAllEventData(events).build();

            response = dataProcessor.sendEvent(eventRequest);

            if (response.hasStatus()) {
                if (response.getStatus().getHandshakeRequired()) {
                    return handshaker.handshake(crawlerId, interval, info.getDataProcessorInfo(), numberOfEvents, 0);
                }
            }

            if (response.hasId()) {
                RecoveryState oldState = stateService.deserialize(interval.getRecoveryState());

                InnerEventId event = null;

                EventID responseId = response.getId();

                long processedEventsCount = events.stream().takeWhile(eventData -> eventData.getEventId().equals(responseId)).count();

                numberOfEvents += processedEventsCount + diff;

                diff = batchSize - processedEventsCount;

                for (EventData eventData: events) {
                    if (eventData.getEventId().equals(responseId)) {
                        Instant startTimeStamp = Instant.ofEpochSecond(eventData.getStartTimestamp().getSeconds(),
                                eventData.getStartTimestamp().getNanos());
                        String id = eventData.getEventId().getId();

                        event = new InnerEventId(startTimeStamp, id);
                        break;
                    }
                }

                if (event != null) {
                    RecoveryState newState;

                    if (oldState == null) {
                        newState = new RecoveryState(
                                event,
                                null,
                                numberOfEvents,
                                0);
                    } else {
                        newState = new RecoveryState(
                                event,
                                oldState.getLastProcessedMessages(),
                                numberOfEvents,
                                oldState.getLastNumberOfMessages());
                    }

                    interval = intervalsWorker.updateRecoveryState(interval, stateService.serialize(newState));
                }
            }

            search = events.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, interval, dataProcessorName, dataProcessorVersion, numberOfEvents, 0);
    }
}
