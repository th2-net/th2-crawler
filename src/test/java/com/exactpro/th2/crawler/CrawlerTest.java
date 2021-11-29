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

import static com.exactpro.th2.crawler.util.TestUtil.doObserverAnswer;
import static com.exactpro.th2.crawler.util.TestUtil.doObserverError;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.Status;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.util.TestUtil;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageData.Builder;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StreamsInfo;

public class CrawlerTest {

    @Test
    @DisplayName("Returns correct value as a sleep timeout when no intervals created")
    public void returnsCorrectSleepTime() throws IOException, UnexpectedDataProcessorException {
        Instant time = Instant.now();
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                time.toString(), DataType.EVENTS, Duration.ofHours(1), Collections.emptySet(), 0, ChronoUnit.MINUTES);
        CrawlerManager manager = new CrawlerManager(configuration);

        Crawler crawler = manager.createCrawler(() -> time.plus(35, ChronoUnit.MINUTES)); // 25 minutes to the interval
        Duration sleep = crawler.process();

        Assertions.assertEquals(Duration.ofMinutes(25), sleep);

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Returns correct value as a sleep timeout when has intervals")
    public void returnsCorrectSleepTimeWhenHasIntervals() throws IOException, UnexpectedDataProcessorException {
        Instant time = Instant.now();
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                time.toString(), DataType.EVENTS, Duration.ofHours(1), Collections.emptySet(), 0, ChronoUnit.MINUTES);
        CrawlerManager manager = new CrawlerManager(configuration);
        Instant end = time.plus(1, ChronoUnit.HOURS);
        manager.getIntervalsWorkerMock().storeInterval(Interval.builder()
                .startTime(time)
                .endTime(end)
                .lastUpdateTime(end)
                .crawlerName(CrawlerManager.NAME)
                .crawlerVersion(CrawlerManager.VERSION)
                .crawlerType(DataType.EVENTS.getTypeName())
                .processed(true)
                .build());

        Crawler crawler = manager.createCrawler(() -> time.plus(60 + 35, ChronoUnit.MINUTES)); // 25 minutes to the next interval
        Duration sleep = crawler.process();

        Assertions.assertEquals(Duration.ofMinutes(25), sleep);

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Calling method process()")
    public void processMethodCall() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, Collections.emptySet());
        CrawlerManager manager = new CrawlerManager(configuration);

        Crawler crawler = manager.createCrawler();
        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));
        verify(manager.getDataProviderMock()).searchEvents(any(EventSearchRequest.class));
    }

    @Test
    public void testCrawlerMessages() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        MessageSearchResponse responses = new MessageSearchResponse(messages);
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class)))
                .then(invk -> {
                    MessageSearchRequest request = invk.getArgument(0);
                    return request.getEndTimestamp().equals(request.getStartTimestamp())
                            ? responses.streamInfoIterator()
                            : responses.iterator();
                });
        doObserverAnswer(MessageResponse.newBuilder().build())
                .when(manager.getDataServiceMock()).sendMessage(
                        any(MessageDataRequest.class),
                        any()
                );

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock(), times(1/*init start IDs*/ + 1/*actual search*/)).searchMessages(any(MessageSearchRequest.class));

        MessageDataRequest expected = createExpectedMessageDataRequest(messages);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected.getMessageDataList().equals(actual.getMessageDataList())), any());
    }

    private static MessageDataRequest createExpectedMessageDataRequest(Collection<Message> messages) {
        return messages.stream()
                .map(MessageSearchResponse::createMessageData)
                .collect(MessageDataRequest::newBuilder, MessageDataRequest.Builder::addMessageData, (b1, b2) -> b1.mergeFrom(b2.build()))
                .build();
    }

    @Test
    @DisplayName("Requiring handshake, getting other name and version")
    public void handshakeNeededAnother() throws IOException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, Collections.emptySet());

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        doObserverAnswer(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build())
                .when(manager.getDataServiceMock()).crawlerConnect(any(CrawlerInfo.class), any());

        TestUtil.<EventDataRequest, EventResponse> doObserverAnswer(request -> {
            List<EventData> events = request.getEventDataList();

            EventID eventID = events.get(events.size() - 1).getEventId();

            return EventResponse.newBuilder().setId(eventID).setStatus(Status.newBuilder().setHandshakeRequired(true).build()).build();
        }).when(manager.getDataServiceMock()).sendEvent(any(EventDataRequest.class), any());

        Assertions.assertThrows(UnexpectedDataProcessorException.class, crawler::process);
    }

    @Test
    @DisplayName("Crawler's actions when a data service fails")
    public void dataServiceFail() throws IOException {
        Instant now = Instant.now();
        Instant from = now.minus(2, ChronoUnit.HOURS);
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                from.toString(), DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler(now);

        String exceptionMessage = "Test exception";

        Builder responseMessage = MessageData.newBuilder()
                .setTimestamp(MessageUtils.toTimestamp(from))
                .setDirectionValue(1).setMessageId(MessageID.newBuilder()
                        .setDirection(Direction.FIRST).setConnectionId(ConnectionID.newBuilder()
                                .setSessionAlias("alias1").build()).setSequence(2).build());
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).then(invocation -> {
            List<StreamResponse> responses = new ArrayList<>();

            MessageSearchRequest request = invocation.getArgument(0);
            MessageID messageId = responseMessage.getMessageId();
            if (!request.getStartTimestamp().equals(request.getEndTimestamp())) {
                StreamResponse response = StreamResponse.newBuilder()
                        .setMessage(responseMessage).build();

                responses.add(response);
                responses.add(StreamResponse.newBuilder()
                        .setStreamInfo(StreamsInfo.newBuilder()
                                .addStreams(Stream.newBuilder()
                                        .setSession(messageId.getConnectionId().getSessionAlias())
                                        .setDirection(messageId.getDirection())
                                        .setLastId(messageId).build())
                                .build())
                        .build());
            } else {
                responses.add(StreamResponse.newBuilder()
                        .setStreamInfo(StreamsInfo.newBuilder()
                                .addStreams(Stream.newBuilder()
                                        .setSession(messageId.getConnectionId().getSessionAlias())
                                        .setDirection(messageId.getDirection())
                                        .setLastId(messageId.toBuilder().setSequence(1)).build())
                                .build())
                        .build());
            }

            return responses.iterator();
        });

        doObserverAnswer(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build())
                .when(manager.getDataServiceMock()).crawlerConnect(any(CrawlerInfo.class), any());

        doObserverError(() -> new RuntimeException(exceptionMessage))
                .when(manager.getDataServiceMock()).sendMessage(any(MessageDataRequest.class), any());

        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, crawler::process, exceptionMessage);
        Assertions.assertTrue(ex.getMessage().contains(exceptionMessage), () -> "Unexpected exception: " + ex);
    }
}
