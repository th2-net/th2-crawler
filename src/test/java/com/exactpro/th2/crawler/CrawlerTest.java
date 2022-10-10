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

import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointers;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled("it doesn't work after th2-4262 refactoring")
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
        Iterator<MessageSearchResponse> iterator = new MessageSearchResponseAdapter(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock()).searchMessages(any(MessageSearchRequest.class));

        MessageDataRequest expected = createExpectedMessageDataRequest(messages);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected.getMessageDataList().equals(actual.getMessageDataList())));
    }

    @Disabled
    @Test
    public void testCrawlerMessagesMaxOutgoingMessageSize() throws IOException, UnexpectedDataProcessorException {
        Message prototype = MessageReaderKt.parseMessage("{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"alias1\"},\"direction\":\"SECOND\",\"sequence\":\"1635664585511283004\"},\"timestamp\":\"2021-10-31T07:18:18.085342Z\",\"messageType\":\"reqType\",\"protocol\":\"prtcl\"}}");
        MessageGroupResponse prototypeResp = MessageSearchResponseAdapter.createMessageGroupResponse(prototype).build();

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Duration.ofHours(1), Set.of(CrawlerManager.SESSIONS), 5, ChronoUnit.MINUTES, prototypeResp.getSerializedSize() * 3);

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        List<Message> messages = List.of(
                modifyMessage(prototype,1, ""),
                modifyMessage(prototype,2, ""),
                modifyMessage(prototype,3, "")
        );
        Iterator<MessageSearchResponse> iterator = new MessageSearchResponseAdapter(messages).iterator();
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

    @Disabled
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
        Iterator<MessageSearchResponse> iterator = new MessageSearchResponseAdapter(messages).iterator();
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
                .map(MessageSearchResponseAdapter::createMessageGroupResponse)
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

            List<com.exactpro.th2.dataprovider.grpc.EventResponse> events = request.getEventDataList();

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

        Message.Builder responseMessage = MessageUtils.message("test", Direction.FIRST, "alias1");
        MessageUtils.setSequence(responseMessage, 2);
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).then(invocation -> {
            List<MessageSearchResponse> responses = new ArrayList<>();

            MessageSearchRequest request = invocation.getArgument(0);
            MessageID messageId = responseMessage.getMetadata().getId();
            MessageSearchResponse pointers = MessageSearchResponse.newBuilder()
                    .setMessageStreamPointers(MessageStreamPointers.newBuilder()
                            .addMessageStreamPointer(MessageStreamPointer.newBuilder()
                                    .setMessageStream(
                                            MessageStream.newBuilder()
                                                    .setName(messageId.getConnectionId().getSessionAlias())
                                                    .setDirection(messageId.getDirection())
                                    )
                                    .setLastId(messageId.toBuilder().setSequence(1)).build())
                            .build())
                    .build();
            if (request.getStartTimestamp().equals(request.getEndTimestamp())) {
                responses.add(pointers);
            } else {
                MessageSearchResponse response = MessageSearchResponse.newBuilder()
                        .setMessage(MessageSearchResponseAdapter.createMessageGroupResponse(responseMessage.build())).build();

                responses.add(response);
                responses.add(pointers);
            }

            return responses.iterator();
        });

        when(manager.getDataServiceMock().crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build());

        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenThrow(new RuntimeException(exceptionMessage));

        Assertions.assertThrows(RuntimeException.class, crawler::process, exceptionMessage);
    }
}
