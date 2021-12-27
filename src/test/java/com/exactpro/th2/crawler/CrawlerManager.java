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

import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.utils.UpdateNotAppliedException;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.util.CrawlerTimeTestImpl;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import org.jetbrains.annotations.NotNull;

public class CrawlerManager {
    public static final String NAME = "test_crawler";
    public static final String VERSION = "1";

    private final DataProcessorService dataServiceMock = mock(DataProcessorService.class);
    private final DataProviderService dataProviderMock = mock(DataProviderService.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    private final IntervalsWorker intervalsWorkerMock = mock(IntervalsWorker.class);
    private final StateService<RecoveryState> stateService = StateService.createFromClasspath(
            RecoveryState.class,
            dataProviderMock,
            null);

    private List<Interval> intervals;
    private List<StreamResponse> searchEventResponse;
    private final CrawlerConfiguration configuration;

    public static final String[] SESSIONS = {"alias1", "alias2"};


    public CrawlerManager(CrawlerConfiguration configuration) throws IOException {
        this.configuration = configuration;
        prepare();
    }

    public DataProcessorService getDataServiceMock() {
        return dataServiceMock;
    }

    public DataProviderService getDataProviderMock() {
        return dataProviderMock;
    }

    public CradleStorage getStorageMock() {
        return storageMock;
    }

    public IntervalsWorker getIntervalsWorkerMock() {
        return intervalsWorkerMock;
    }

    public StateService<RecoveryState> getStateService() {
        return stateService;
    }

    @NotNull
    public Crawler createCrawler() throws IOException {
        CrawlerMetrics metrics = mock(CrawlerMetrics.class);
        when(metrics.measureTime(any(DataType.class), any(CrawlerMetrics.Method.class), any())).then(invk ->
                invk.<CrawlerMetrics.CrawlerDataOperation<?>>getArgument(2).call());
        CrawlerContext crawlerContext = new CrawlerContext()
                .setCrawlerTime(new CrawlerTimeTestImpl())
                .setMetrics(metrics);
        return new Crawler(stateService, storageMock, dataServiceMock, dataProviderMock, configuration, crawlerContext);
    }

    public static CrawlerConfiguration createConfig(String from, DataType dataType, Set<String> sessions, SessionAliasType type) {
        return new CrawlerConfiguration(from, null, NAME,
                dataType, "PT1H", 1, ChronoUnit.NANOS, 1, 10, 5,
                ChronoUnit.MINUTES, true, sessions, type);
    }

    private void prepare() throws IOException {
        intervals = new ArrayList<>();
        searchEventResponse = addEvents();

        prepareDataProvider();
        prepareDataService();

        when(storageMock.getIntervalsWorker()).thenReturn(intervalsWorkerMock);
        prepareIntervalWorkers();
    }

    private void prepareDataProvider() {
        when(dataProviderMock.getEvent(any(EventID.class)))
                .thenReturn(EventData.newBuilder().setEventId(toEventID("2")).setEventName("0.0.1|ProviderTest|"+ Instant.now()).build());

        when(dataProviderMock.searchEvents(any(EventSearchRequest.class)))
                .thenAnswer(invocation -> searchEventResponse.stream().filter(streamResponse -> {
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
    }

    private void prepareDataService() {
        when(dataServiceMock.crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataProcessorInfo.newBuilder().setName(NAME).setVersion(VERSION).build());

        when(dataServiceMock.sendEvent(any(EventDataRequest.class))).then(invocation -> {
            EventDataRequest request = invocation.getArgument(0);

            List<EventData> events = request.getEventDataList();

            EventID eventID = events.get(events.size() - 1).getEventId();

            return EventResponse.newBuilder().setId(eventID).build();
        });
    }

    private void prepareIntervalWorkers() throws IOException {
        mockWhenUpdateRecoveryState();
        mockWhenStoreInterval();
        mockWhenSetIntervalProcessed();
        mockWhenGetIntervals();
    }

    private void mockWhenGetIntervals() throws IOException {
        when(intervalsWorkerMock.getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString())).thenReturn(intervals);
    }

    private void mockWhenUpdateRecoveryState() throws IOException {
        when(intervalsWorkerMock.updateRecoveryState(any(Interval.class), anyString())).then(invocation -> {
            Interval interval = invocation.getArgument(0);
            RecoveryState state = stateService.deserialize(invocation.getArgument(1));

            List<Interval> intervalList = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).collect(Collectors.toList());

            Interval storedInterval = intervalList.get(0);

            if (!storedInterval.getLastUpdateDateTime().equals(interval.getLastUpdateDateTime()))
                throw new UpdateNotAppliedException("Failed to update recovery state at interval from "
                        +interval.getStartTime()+", to "+interval.getEndTime());

            interval.setRecoveryState(stateService.serialize(state));
            interval.setLastUpdateDateTime(Instant.now());

            return interval;
        });
    }

    private void mockWhenStoreInterval () throws IOException {
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
    }

    private void mockWhenSetIntervalProcessed() throws IOException {
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
}
