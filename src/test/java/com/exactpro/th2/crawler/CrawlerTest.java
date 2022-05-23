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

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalBuilder;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.Status;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;

import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageData.Builder;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StreamsInfo;

import io.grpc.internal.GrpcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

import static com.exactpro.th2.crawler.TestUtilKt.createMessageID;
import static com.exactpro.th2.crawler.TestUtilKt.createRecoveryState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        Iterator<StreamResponse> iterator = new MessageSearchResponse(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock()).searchMessages(any(MessageSearchRequest.class));

        MessageDataRequest expected = createExpectedMessageDataRequest(messages);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected.getMessageDataList().equals(actual.getMessageDataList())));
    }

    @Test
    public void testCrawlerMessagesMaxOutgoingMessageSize() throws IOException, UnexpectedDataProcessorException {
        Message prototype = MessageReaderKt.parseMessage("{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"alias1\"},\"direction\":\"SECOND\",\"sequence\":\"1635664585511283004\"},\"timestamp\":\"2021-10-31T07:18:18.085342Z\",\"messageType\":\"reqType\",\"protocol\":\"prtcl\"}}");

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Duration.ofHours(1), Set.of(CrawlerManager.SESSIONS), 5, ChronoUnit.MINUTES, prototype.getSerializedSize() * 3);

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        List<Message> messages = List.of(
                modifyMessage(prototype,1, ""),
                modifyMessage(prototype,2, ""),
                modifyMessage(prototype,3, "")
        );
        Iterator<StreamResponse> iterator = new MessageSearchResponse(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock()
                .sendMessage(any(MessageDataRequest.class)))
                .thenReturn(MessageResponse.newBuilder() // This code is requred for internal state verification
                                .addIds(createMessageID("alias1", Direction.SECOND, 1))
                                .build(),
                        MessageResponse.newBuilder()
                                .addIds(createMessageID("alias1", Direction.SECOND, 3))
                                .build());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock()).searchMessages(any(MessageSearchRequest.class));

        verify(manager.getDataServiceMock(), times(2)).sendMessage(any());
        verify(manager.getDataServiceMock(), times(1))
                .sendMessage(argThat(actual -> createExpectedMessageDataRequest(messages.subList(0, 2)).getMessageDataList().equals(actual.getMessageDataList())));
        verify(manager.getDataServiceMock(), times(1))
                .sendMessage(argThat(actual -> createExpectedMessageDataRequest(messages.subList(2, 3)).getMessageDataList().equals(actual.getMessageDataList())));

        // FIXME: Implement verification of state
//        verify(manager.getStorageMock().getIntervalsWorker(), times(1)).updateRecoveryStateAsync(any(), anyString());
    }

    @Test
    public void testCrawlerMessagesMaxOutgoingMessageSizeExceeded() throws IOException, UnexpectedDataProcessorException {
        Message prototype = MessageReaderKt.parseMessage("{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"alias1\"},\"direction\":\"SECOND\",\"sequence\":\"1635664585511283004\"},\"timestamp\":\"2021-10-31T07:18:18.085342Z\",\"messageType\":\"reqType\",\"protocol\":\"prtcl\"}}");

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Duration.ofHours(1), Set.of(CrawlerManager.SESSIONS), 5, ChronoUnit.MINUTES, prototype.getSerializedSize() * 2);

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        List<Message> messages = List.of(
                modifyMessage(prototype,1, StringUtils.repeat("a", prototype.getSerializedSize() * 2))
        );
        Iterator<StreamResponse> iterator = new MessageSearchResponse(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        Assertions.assertThrows(IllegalStateException.class, crawler::process);
    }

    private Message modifyMessage(Message prototype, long sequence, String content) {
        Message.Builder builder = prototype.toBuilder();
        MessageUtils.setSequence(builder, sequence);
        builder.putFields("content", Value.newBuilder().setSimpleValue(content).build());
        return builder.build();
    }

    private static MessageDataRequest createExpectedMessageDataRequest(Collection<Message> messages) {
        return messages.stream()
                .map(MessageData.newBuilder()::setMessage)
                .collect(MessageDataRequest::newBuilder, MessageDataRequest.Builder::addMessageData, (b1, b2) -> b1.mergeFrom(b2.build()))
                .build();
    }

    @Test
    @DisplayName("Requiring handshake, getting other name and version")
    public void handshakeNeededAnother() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, Collections.emptySet());

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        when(manager.getDataServiceMock().crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build());

        when(manager.getDataServiceMock().sendEvent(any(EventDataRequest.class))).then(invocation -> {
            EventDataRequest request = invocation.getArgument(0);

            List<EventData> events = request.getEventDataList();

            EventID eventID = events.get(events.size() - 1).getEventId();

            return EventResponse.newBuilder().setId(eventID).setStatus(Status.newBuilder().setHandshakeRequired(true).build()).build();
        });

        Assertions.assertThrows(UnexpectedDataProcessorException.class, crawler::process);
    }

    @Test
    @DisplayName("Crawler's actions when a data service fails")
    public void dataServiceFail() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        String exceptionMessage = "Test exception";

        Builder responseMessage = MessageData.newBuilder()
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

        when(manager.getDataServiceMock().crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build());

        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenThrow(new RuntimeException(exceptionMessage));

        Assertions.assertThrows(RuntimeException.class, crawler::process, exceptionMessage);
    }

    @Test
    @DisplayName("Single Crawler takes unprocessed interval")
    public void unprocessedIntervalSingleMode() throws IOException, UnexpectedDataProcessorException {
        String from = "2021-06-16T12:00:00.00Z";
        String to = "2021-06-16T20:00:00.00Z";
        String intervalEnd = "2021-06-16T13:00:00.00Z";

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                from, to, DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        StateService<RecoveryState> stateService =
                StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);

        var state = createRecoveryState();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        Interval interval = new IntervalBuilder()
                .crawlerName("name")
                .crawlerVersion("v1")
                .crawlerType("MESSAGES")
                .startTime(Instant.parse(from))
                .endTime(Instant.parse(intervalEnd))
                .processed(false)
                .recoveryState(stateService.serialize(state))
                .build();

        manager.getIntervalsWorkerMock().storeInterval(interval);

        Iterator<StreamResponse> iterator = new MessageSearchResponse(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        Assertions.assertTrue(interval.isProcessed());
    }

    @Test
    @DisplayName("Crawler takes unprocessed interval in MULTIPLE mode with FIXED endTime")
    public void unprocessedIntervalMultipleModeFixedEnd() throws IOException, UnexpectedDataProcessorException {
        String from = "2021-06-16T12:00:00.00Z";
        String to = "2021-06-16T20:00:00.00Z";
        String intervalEnd1 = "2021-06-16T13:00:00.00Z";
        String intervalEnd2 = "2021-06-16T14:00:00.00Z";
        String lastUpdateTime1 = "2021-06-16T11:59:59.00Z";
        String lastUpdateTime2 = "2021-06-16T10:00:00.00Z";

        CrawlerConfiguration configuration = new CrawlerConfiguration(
                from, to, "test_crawler", DataType.MESSAGES, "PT1H", 1, ChronoUnit.NANOS,
                1, 10, 5, ChronoUnit.MINUTES, false,
                Set.of(CrawlerManager.SESSIONS), GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE);

        CrawlerManager manager = new CrawlerManager(configuration);


        Crawler crawler = manager.createCrawler();

        StateService<RecoveryState> stateService =
                StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);

        var state = createRecoveryState();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        Interval interval1 = new IntervalBuilder()
                .crawlerName("name")
                .crawlerVersion("v1")
                .crawlerType("MESSAGES")
                .startTime(Instant.parse(from))
                .endTime(Instant.parse(intervalEnd1))
                .processed(false)
                .recoveryState(stateService.serialize(state))
                .build();

        Interval interval2 = new IntervalBuilder()
                .crawlerName("name")
                .crawlerVersion("v1")
                .crawlerType("MESSAGES")
                .startTime(Instant.parse(intervalEnd1))
                .endTime(Instant.parse(intervalEnd2))
                .processed(false)
                .recoveryState(stateService.serialize(state))
                .build();

        manager.getIntervalsWorkerMock().storeInterval(interval1);
        manager.getIntervalsWorkerMock().storeInterval(interval2);

        interval1.setLastUpdateDateTime(Instant.parse(lastUpdateTime1));
        interval2.setLastUpdateDateTime(Instant.parse(lastUpdateTime2));

        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(new MessageSearchResponse(messages).iterator());
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        Assertions.assertTrue(interval1.isProcessed());

        // verify that no new intervals were created
        verify(manager.getIntervalsWorkerMock(), times(2)).storeInterval(any(Interval.class));
    }

    @Test
    @DisplayName("Crawler takes unprocessed interval in MULTIPLE mode with FLOATING endTime")
    public void unprocessedIntervalMultipleModeFloatingEnd() throws IOException, UnexpectedDataProcessorException {
        String from = "2021-06-16T12:00:00.00Z";
        String intervalEnd1 = "2021-06-16T13:00:00.00Z";
        String intervalEnd2 = "2021-06-16T14:00:00.00Z";
        String lastUpdateTime1 = "2021-06-16T11:59:59.00Z";
        String lastUpdateTime2 = "2021-06-16T10:00:00.00Z";

        CrawlerConfiguration configuration = new CrawlerConfiguration(
                from, null, "test_crawler", DataType.MESSAGES, "PT1H", 1, ChronoUnit.NANOS,
                1, 10, 5, ChronoUnit.MINUTES, false,
                Set.of(CrawlerManager.SESSIONS), GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE);

        CrawlerManager manager = new CrawlerManager(configuration);


        Crawler crawler = manager.createCrawler();

        StateService<RecoveryState> stateService =
                StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);

        var state = createRecoveryState();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        Interval interval1 = new IntervalBuilder()
                .crawlerName("name")
                .crawlerVersion("v1")
                .crawlerType("MESSAGES")
                .startTime(Instant.parse(from))
                .endTime(Instant.parse(intervalEnd1))
                .processed(false)
                .recoveryState(stateService.serialize(state))
                .build();

        Interval interval2 = new IntervalBuilder()
                .crawlerName("name")
                .crawlerVersion("v1")
                .crawlerType("MESSAGES")
                .startTime(Instant.parse(intervalEnd1))
                .endTime(Instant.parse(intervalEnd2))
                .processed(false)
                .recoveryState(stateService.serialize(state))
                .build();

        manager.getIntervalsWorkerMock().storeInterval(interval1);
        manager.getIntervalsWorkerMock().storeInterval(interval2);

        interval1.setLastUpdateDateTime(Instant.parse(lastUpdateTime1));
        interval2.setLastUpdateDateTime(Instant.parse(lastUpdateTime2));

        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(new MessageSearchResponse(messages).iterator());
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        Assertions.assertTrue(interval1.isProcessed());

        // verify that no new intervals were created
        verify(manager.getIntervalsWorkerMock(), times(2)).storeInterval(any(Interval.class));
    }
}
