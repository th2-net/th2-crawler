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

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.crawler.AbstractStrategy;
import com.exactpro.th2.crawler.Action;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.CrawlerData;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.Report;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo.Builder;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.EventPart;
import com.exactpro.th2.crawler.events.strategy.EventsCrawlerData.ResumeEventId;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.Method;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod;
import com.exactpro.th2.crawler.state.v2.InnerEventId;
import com.exactpro.th2.crawler.state.v2.RecoveryState;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.CrawlerUtils.EventsSearchParameters;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse;
import com.google.protobuf.Timestamp;
import kotlin.Pair;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static java.util.Objects.requireNonNull;

public class EventsStrategy extends AbstractStrategy<ResumeEventId, EventPart> {
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
        if (StringUtils.isBlank(config.getBook())) {
            throw new IllegalArgumentException("The 'book' property in configuration can not be blank");
        }
        if (config.getScopes().isEmpty()) {
            throw new IllegalArgumentException("The 'scopes' property in configuration can not be empty");
        }
    }

    @Override
    public void setupIntervalInfo(@NotNull Builder info, @Nullable RecoveryState state) {
        requireNonNull(info, "'info' parameter");
        InnerEventId lastProcessedEvent = state == null ? null : state.getLastProcessedEvent();
        if (lastProcessedEvent != null) {
            info.setLastEventId(EventUtils.toEventID(
                    lastProcessedEvent.getStartTimestamp(),
                    lastProcessedEvent.getBook(),
                    lastProcessedEvent.getScope(),
                    lastProcessedEvent.getId())
            );
        }
    }

    @NotNull
    @Override
    public CrawlerData<ResumeEventId, EventPart> requestData(
            @NotNull Timestamp start,
             @NotNull Timestamp end,
             @NotNull DataParameters parameters,
             @Nullable EventsCrawlerData.ResumeEventId prevResult
    ) {
        requireNonNull(start, "'start' parameter");
        requireNonNull(end, "'end' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        EventID resumeId = getResumeId(prevResult);
        return new EventsCrawlerData(
                CrawlerUtils.searchEvents(
                        provider,
                        new EventsSearchParameters(start, end, config.getBook(), config.getScopes(), resumeId), metrics),
                parameters.getCrawlerId(),
                config.getMaxOutgoingDataSize()
        );
    }

    @NotNull
    @Override
    public Report<ResumeEventId> processData(
            @NotNull DataProcessorService processor,
            @NotNull InternalInterval interval,
            @NotNull DataParameters parameters,
            @NotNull EventPart data,
            @Nullable ResumeEventId prevCheckpoint) {
        requireNonNull(processor, "'processor' parameter");
        requireNonNull(interval, "'interval' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        requireNonNull(data, "'data' parameter");

        Interval original = interval.getOriginal();

        EventDataRequest eventRequest = data.getRequest();

        List<EventResponse> events = eventRequest.getEventDataList();
        if (events.isEmpty()) {
            LOGGER.info("No more events in interval from: {}, to: {}", original.getStart(), original.getEnd());
            return Report.empty();
        }

        com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse response = sendEventsToProcessor(processor, eventRequest);

        if (response.hasStatus()) {
            if (response.getStatus().getHandshakeRequired()) {
                return Report.handshake();
            }
        }

        boolean hasCheckpoint = response.hasId();
        var countAndCheckpoint = processServiceResponse(hasCheckpoint ? response.getId() : null, events);

        ResumeEventId continuation = null;
        if (hasCheckpoint) {
            EventResponse checkpointEvent = countAndCheckpoint.getSecond();
            if (checkpointEvent != null) {
                continuation = new ResumeEventId(checkpointEvent.getEventId());
            }
        }

        return new Report<>(Action.CONTINUE, continuation);
    }

    @Nullable
    @Override
    public EventsCrawlerData.ResumeEventId continuationFromState(@NotNull RecoveryState state) {
        requireNonNull(state, "'state' parameter");
        InnerEventId innerId = state.getLastProcessedEvent();
        return innerId == null ? null : new ResumeEventId(
                EventUtils.toEventID(
                    innerId.getStartTimestamp(),
                    innerId.getBook(),
                    innerId.getScope(),
                    innerId.getId()
                )
        );
    }

    @NotNull
    @Override
    public RecoveryState continuationToState(@Nullable RecoveryState current, @NotNull EventsCrawlerData.ResumeEventId continuation, long processedData) {
        requireNonNull(continuation, "'continuation' parameter");
        Function<ResumeEventId, InnerEventId> toInnerId = cont ->
                new InnerEventId(
                        cont.getResumeId().getBookName(),
                        cont.getResumeId().getScope(),
                        toInstant(cont.getResumeId().getStartTimestamp()),
                        cont.getResumeId().getId()
                );
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
                current.getProcessedMessages()
        );
    }

    private com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse sendEventsToProcessor(DataProcessorService dataProcessor, EventDataRequest eventRequest) {
        com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse response = metrics.measureTime(DataType.EVENTS, Method.PROCESS_DATA, () -> dataProcessor.sendEvent(eventRequest));
        metrics.processorMethodInvoked(ProcessorMethod.SEND_EVENT);
        return response;
    }

    private Pair<@NotNull Integer, @Nullable EventResponse> processServiceResponse(@Nullable EventID checkpoint, List<EventResponse> events) {
        if (checkpoint == null) {
            return new Pair<>(events.size(), null);
        }
        int processed = 0;
        EventResponse checkpointEvent = null;
        for (EventResponse event : events) {
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

    @Nullable
    private static EventID getResumeId(@Nullable EventsCrawlerData.ResumeEventId continuation) {
        return continuation == null ? null : continuation.getResumeId();
    }
}
