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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.intervals.RecoveryState;
import com.exactpro.cradle.utils.UpdateNotAppliedException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.dataservice.grpc.*;
import com.exactpro.th2.crawler.exception.UnexpectedDataServiceException;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.exactpro.th2.crawler.util.CrawlerTimeTestImpl;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestClass {

    private final String name = "test_crawler";
    private final String version = "1";

    private final DataServiceService dataServiceMock = mock(DataServiceService.class);
    private final DataProviderService dataProviderMock = mock(DataProviderService.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    private final IntervalsWorker intervalsWorkerMock = mock(IntervalsWorker.class);

    private List<Interval> intervals;
    private List<StreamResponse> searchEventResponse;

    private final String[] aliases = new String[] {"alias1", "alias2"};

    @BeforeEach
    private void prepare() throws IOException {
        intervals = new ArrayList<>();
        searchEventResponse = addEvents();

        when(dataProviderMock.getEvent(any(EventID.class)))
                .thenReturn(EventData.newBuilder().setEventId(toEventID("2")).setEventName("0.0.1|ProviderTest|"+Instant.now()).build());

        when(dataProviderMock.searchEvents(any(EventSearchRequest.class))).thenAnswer(invocation -> searchEventResponse.stream().filter(streamResponse -> {
            if (streamResponse.hasEvent()) {
                EventSearchRequest searchRequest = invocation.getArgument(0);

                long responseSeconds = streamResponse.getEvent().getStartTimestamp().getSeconds();
                long responseNanos = streamResponse.getEvent().getStartTimestamp().getNanos();

                Instant responseTime = Instant.ofEpochSecond(responseSeconds, responseNanos);

                long fromSearchSeconds = searchRequest.getStartTimestamp().getSeconds();
                long fromSearchNanos = searchRequest.getStartTimestamp().getNanos();

                Instant from = Instant.ofEpochSecond(fromSearchSeconds, fromSearchNanos);

                long toSearchSeconds = searchRequest.getEndTimestamp().getSeconds();
                long toSearchNanos = searchRequest.getEndTimestamp().getNanos();

                Instant to = Instant.ofEpochSecond(toSearchSeconds, toSearchNanos);


                return responseTime.compareTo(from) >= 0 && responseTime.compareTo(to) <= 0;
            }
            return false;
        }).collect(Collectors.toList()).iterator());

        when(dataServiceMock.crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataServiceInfo.newBuilder().setEventId(toEventID("3")).setName(name).setVersion(version).build());

        when(dataServiceMock.sendEvent(any(EventDataRequest.class))).then(invocation -> {
                    EventDataRequest request = invocation.getArgument(0);

                    List<EventData> events = request.getEventDataList();

                    EventID eventID = events.get(events.size() - 1).getEventId();

                    return EventResponse.newBuilder().setId(eventID).build();
                });

        when(storageMock.getIntervalsWorker()).thenReturn(intervalsWorkerMock);

        when(intervalsWorkerMock.getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString())).thenReturn(intervals);

        when(intervalsWorkerMock.updateRecoveryState(any(Interval.class), any(RecoveryState.class))).then(invocation -> {
            Interval interval = invocation.getArgument(0);
            RecoveryState state = invocation.getArgument(1);

            List<Interval> intervalList = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).collect(Collectors.toList());

            Interval storedInterval = intervalList.get(0);

            if (!storedInterval.getLastUpdateDateTime().equals(interval.getLastUpdateDateTime()))
                throw new UpdateNotAppliedException("Failed to update recovery state at interval from "
                        +interval.getStartTime()+", to "+interval.getEndTime());

            interval.setRecoveryState(state);
            interval.setLastUpdateDateTime(Instant.now());

            return interval;
        });

        when(intervalsWorkerMock.storeInterval(any(Interval.class))).then(invocation -> {
            Interval interval = invocation.getArgument(0);

            long res = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).count();

            if (res == 0) {
                intervals.add(interval);
                interval.setLastUpdateDateTime(Instant.now());
                return true;
            }

            return false;
        });

        when(intervalsWorkerMock.setIntervalProcessed(any(Interval.class), anyBoolean())).then(invocation -> {
            Interval interval = invocation.getArgument(0);
            boolean isProcessed = invocation.getArgument(1);

            List<Interval> intervalList = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).collect(Collectors.toList());

            Interval storedInterval = intervalList.get(0);

            if (!storedInterval.getLastUpdateDateTime().equals(interval.getLastUpdateDateTime()))
                throw new UpdateNotAppliedException("Failed to set processed flag to "+isProcessed+" at interval from "
                        +interval.getStartTime()+", to "+interval.getEndTime());

            interval.setProcessed(isProcessed);
            interval.setLastUpdateDateTime(Instant.now());

            return interval;
        });
    }

    private List<StreamResponse> addEvents() {
        CrawlerConfiguration configuration = new CrawlerConfiguration("2021-06-16T12:00:00.00Z", null, name,
                "EVENTS", "PT1H", 1, ChronoUnit.NANOS, 1, 10, 5,
                ChronoUnit.MINUTES, 10, ChronoUnit.MINUTES,true, new HashSet<>(), null);

        List<StreamResponse> responses = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            StreamResponse response = StreamResponse.newBuilder()
                    .setEvent(EventData.newBuilder()
                            .setEventId(toEventID(i +"id"))
                            .setEventName("0.0.1|SearchProviderDataTest|"+Instant.now())
                            .setStartTimestamp(toTimestamp(Instant.parse(configuration.getFrom()).plus(i * 10, ChronoUnit.MINUTES)))
                            .build())
                    .build();

            responses.add(response);
        }

        return responses;
    }

    @Test
    @DisplayName("Calling method process()")
    public void processMethodCall() throws IOException, UnexpectedDataServiceException {
        CrawlerConfiguration configuration = new CrawlerConfiguration("2021-06-16T12:00:00.00Z", null, name,
                "EVENTS", "PT1H", 1, ChronoUnit.NANOS, 1, 10, 5,
                ChronoUnit.MINUTES, 10, ChronoUnit.MINUTES, true, new HashSet<>(), null);

        Crawler crawler = new Crawler(storageMock, dataServiceMock, dataProviderMock, configuration, new CrawlerTimeTestImpl());

        crawler.process();

        verify(intervalsWorkerMock).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(intervalsWorkerMock).storeInterval(any(Interval.class));

        if (configuration.getType().equals("EVENTS")) {
            verify(dataProviderMock).searchEvents(any(EventSearchRequest.class));
        } else if (configuration.getType().equals("MESSAGES")) {
            verify(dataProviderMock).searchMessages(any(MessageSearchRequest.class));
        }
    }

    @Test
    @DisplayName("Requiring handshake, getting other name and version")
    public void handshakeNeededAnother() {
        CrawlerConfiguration configuration = new CrawlerConfiguration("2021-06-16T12:00:00.00Z", null, name,
                "EVENTS", "PT1H", 1, ChronoUnit.NANOS, 1, 10, 5,
                ChronoUnit.MINUTES,10, ChronoUnit.MINUTES, true, new HashSet<>(), null);

        Crawler crawler = new Crawler(storageMock, dataServiceMock, dataProviderMock, configuration, new CrawlerTimeTestImpl());

        when(dataServiceMock.crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataServiceInfo.newBuilder().setEventId(toEventID("3")).setName("another_crawler").setVersion(version).build());

        when(dataServiceMock.sendEvent(any(EventDataRequest.class))).then(invocation -> {
            EventDataRequest request = invocation.getArgument(0);

            List<EventData> events = request.getEventDataList();

            EventID eventID = events.get(events.size() - 1).getEventId();

            return EventResponse.newBuilder().setId(eventID).setStatus(Status.newBuilder().setHandshakeRequired(true).build()).build();
        });

        Assertions.assertThrows(UnexpectedDataServiceException.class, crawler::process);
    }

    @Test
    @DisplayName("Crawler's actions when a data service fails")
    public void dataServiceFail() {
        CrawlerConfiguration configuration = new CrawlerConfiguration("2021-06-16T12:00:00.00Z", null, name,
                "MESSAGES", "PT1H", 1, ChronoUnit.NANOS, 1, 10, 5,
                ChronoUnit.MINUTES,10, ChronoUnit.MINUTES, true, new HashSet<>(Arrays.asList(aliases)), null);

        Crawler crawler = new Crawler(storageMock, dataServiceMock, dataProviderMock, configuration, new CrawlerTimeTestImpl());

        String exceptionMessage = "Test exception";

        when(dataProviderMock.searchMessages(any(MessageSearchRequest.class))).then(invocation -> {
            List<StreamResponse> responses = new ArrayList<>();

            StreamResponse response = StreamResponse.newBuilder()
                    .setMessage(MessageData.newBuilder()
                            .setDirectionValue(1).setBody("").setMessageId(MessageID.newBuilder()
                                    .setDirection(Direction.FIRST).setConnectionId(ConnectionID.newBuilder()
                                            .setSessionAlias("alias1").build()).setSequence(1).build())).build();

            responses.add(response);

            return responses.iterator();
        });

        when(dataServiceMock.crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataServiceInfo.newBuilder().setEventId(toEventID("3")).setName("another_crawler").setVersion(version).build());

        when(dataServiceMock.sendMessage(any(MessageDataRequest.class))).thenThrow(new RuntimeException(exceptionMessage));

        Assertions.assertThrows(RuntimeException.class, crawler::process, exceptionMessage);
    }
}
