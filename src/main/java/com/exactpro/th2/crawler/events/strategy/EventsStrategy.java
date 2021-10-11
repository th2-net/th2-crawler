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

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.crawler.AbstractStrategy;
import com.exactpro.th2.crawler.Action;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.Report;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo.Builder;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.ResumeEventId;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.CrawlerUtils.EventsSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.google.protobuf.Timestamp;

import kotlin.Pair;

public class EventsStrategy extends AbstractStrategy<EventsCrawlerData, ResumeEventId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventsStrategy.class);

    private final DataProviderService provider;
    private final CrawlerConfiguration config;

    public EventsStrategy(
            DataProviderService provider,
            CrawlerMetrics metrics,
            CrawlerConfiguration config
    ) {
        super(metrics);
        this.provider = Objects.requireNonNull(provider, "'Provider' parameter");
        this.config = Objects.requireNonNull(config, "'Config' parameter");
    }

    @Override
    public void setupIntervalInfo(@NotNull Builder info, @Nullable RecoveryState state) {
        requireNonNull(info, "'info' parameter");
        InnerEventId lastProcessedEvent = state == null ? null : state.getLastProcessedEvent();
        if (lastProcessedEvent != null) {
            info.setLastEventId(EventUtils.toEventID(lastProcessedEvent.getId()));
        }
    }

    @NotNull
    @Override
    public EventsCrawlerData requestData(@NotNull Timestamp start, @NotNull Timestamp end, @NotNull DataParameters parameters,
                                         @Nullable EventsCrawlerData.ResumeEventId prevResult) {
        requireNonNull(start, "'start' parameter");
        requireNonNull(end, "'end' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        EventID resumeId = getResumeId(prevResult);
        int batchSize = config.getBatchSize();
        SearchResult<EventData> result = CrawlerUtils.searchEvents(provider,
                new EventsSearchParameters(start, end, batchSize, resumeId), metrics);
        List<EventData> events = result.getData();
        if (events.isEmpty()) {
            LOGGER.info("No more events in interval from: {}, to: {}", start, end);
        }
        return new EventsCrawlerData(events,
                events.isEmpty() ? null : resumeIdFromEvent(events.get(events.size() - 1)),
                events.size() == batchSize
        );
    }

    @NotNull
    @Override
    public Report<ResumeEventId> processData(
            @NotNull DataProcessorService processor,
            @NotNull InternalInterval interval,
            @NotNull DataParameters parameters,
            @NotNull EventsCrawlerData data
    ) {
        requireNonNull(processor, "'processor' parameter");
        requireNonNull(interval, "'interval' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        requireNonNull(data, "'data' parameter");

        CrawlerId crawlerId = parameters.getCrawlerId();
        Interval original = interval.getOriginal();

        List<EventData> events = data.getData();

        if (events.isEmpty()) {
            LOGGER.info("No more events in interval from: {}, to: {}", original.getStartTime(), original.getEndTime());
            return Report.empty();
        }

        EventDataRequest.Builder eventRequestBuilder = EventDataRequest.newBuilder();
        EventDataRequest eventRequest = eventRequestBuilder.setId(crawlerId).addAllEventData(events).build();

        EventResponse response = sendEventsToProcessor(processor, eventRequest);

        if (response.hasStatus()) {
            if (response.getStatus().getHandshakeRequired()) {
                return Report.handshake();
            }
        }

        boolean hasCheckpoint = response.hasId();
        var countAndCheckpoint = processServiceResponse(hasCheckpoint ? response.getId() : null, events);

        long processedEventsCount = countAndCheckpoint.getFirst();
        long remaining = events.size() - processedEventsCount;

        ResumeEventId continuation = null;
        if (hasCheckpoint) {
            EventData checkpointEvent = countAndCheckpoint.getSecond();
            if (checkpointEvent != null) {
                Instant startTimeStamp = CrawlerUtils.fromTimestamp(checkpointEvent.getStartTimestamp());
                continuation = new ResumeEventId(checkpointEvent.getEventId(), startTimeStamp);
            }
        }

        return new Report<>(Action.CONTINUE, processedEventsCount, remaining, continuation);
    }

    @Nullable
    @Override
    public EventsCrawlerData.ResumeEventId continuationFromState(@NotNull RecoveryState state) {
        requireNonNull(state, "'state' parameter");
        InnerEventId innerId = state.getLastProcessedEvent();
        return innerId == null ? null : new ResumeEventId(
                EventUtils.toEventID(innerId.getId()),
                innerId.getStartTimestamp()
        );
    }

    @NotNull
    @Override
    public RecoveryState continuationToState(@Nullable RecoveryState current, @NotNull EventsCrawlerData.ResumeEventId continuation, long processedData) {
        requireNonNull(continuation, "'continuation' parameter");
        Function<ResumeEventId, InnerEventId> toInnerId = cont ->
                new InnerEventId(cont.getTimestamp(), cont.getResumeId().getId());
        if (current == null) {
            return new RecoveryState(
                    toInnerId.apply(continuation),
                    null,
                    processedData,
                    0
            );
        }
        return new RecoveryState(
                toInnerId.apply(continuation),
                current.getLastProcessedMessages(),
                processedData,
                current.getLastNumberOfMessages()
        );
    }

    private EventResponse sendEventsToProcessor(DataProcessorService dataProcessor, EventDataRequest eventRequest) {
        EventResponse response = dataProcessor.sendEvent(eventRequest);
        metrics.processorMethodInvoked(ProcessorMethod.SEND_EVENT);
        return response;
    }

    private Pair<@NotNull Integer, @Nullable EventData> processServiceResponse(@Nullable EventID checkpoint, List<EventData> events) {
        if (checkpoint == null) {
            return new Pair<>(events.size(), null);
        }
        int processed = 0;
        EventData checkpointEvent = null;
        for (EventData event : events) {
            if (checkpointEvent == null) {
                processed++;
            }
            if (event.getEventId().equals(checkpoint)) {
                checkpointEvent = event;
            }
        }
        if (!events.isEmpty()) {
            metrics.lastEvent(events.get(events.size() - 1));
        }
        return new Pair<>(processed, checkpointEvent);
    }

    private static ResumeEventId resumeIdFromEvent(EventData data) {
        return new ResumeEventId(data.getEventId(), CrawlerUtils.fromTimestamp(data.getStartTimestamp()));
    }

    @Nullable
    private static EventID getResumeId(@Nullable EventsCrawlerData.ResumeEventId continuation) {
        return continuation == null ? null : continuation.getResumeId();
    }
}
