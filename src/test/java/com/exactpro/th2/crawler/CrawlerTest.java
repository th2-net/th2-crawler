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

import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageData.Builder;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StreamsInfo;

import com.exactpro.th2.dataprovider.grpc.StringList;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrawlerTest {

    @Test
    @DisplayName("Calling method process()")
    public void processMethodCall() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, Collections.emptySet(), SessionAliasType.PLAIN_TEXT);
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
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS), SessionAliasType.PLAIN_TEXT);

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        Iterator<StreamResponse> iterator = new MessageSearchResponse(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.newBuilder().build());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock()).searchMessages(any(MessageSearchRequest.class));

        MessageDataRequest expected = createExpectedMessageDataRequest(messages);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected.getMessageDataList().equals(actual.getMessageDataList())));
    }

    private static MessageDataRequest createExpectedMessageDataRequest(Collection<Message> messages) {
        return messages.stream()
                .map(MessageData.newBuilder()::setMessage)
                .collect(MessageDataRequest::newBuilder, MessageDataRequest.Builder::addMessageData, (b1, b2) -> b1.mergeFrom(b2.build()))
                .build();
    }

    @Test
    @DisplayName("Requiring handshake, getting other name and version")
    public void handshakeNeededAnother() throws IOException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, Collections.emptySet(), SessionAliasType.PLAIN_TEXT);

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
    public void dataServiceFail() throws IOException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Set.of(CrawlerManager.SESSIONS), SessionAliasType.PLAIN_TEXT);

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
    @DisplayName("Crawler gets extra aliases while processing intervals")
    public void dynamicAliasesProcessing() throws IOException, UnexpectedDataProcessorException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Set.of("alias\\d+"), SessionAliasType.REGEXP
        );

        CrawlerManager manager = new CrawlerManager(configuration);

        List<Message> messages1 = MessageReaderKt.readMessages(Paths.get("src/test/resources/dynamic_aliases_messages-1.txt"));
        List<Message> messages2 = MessageReaderKt.readMessages(Paths.get("src/test/resources/dynamic_aliases_messages-2.txt"));

        List<String> aliases1 = List.of("alias1", "alias2");
        StringList list1 = StringList.newBuilder().addAllListString(aliases1).build();

        List<String> aliases2 = new ArrayList<>(List.of("alias1", "alias2", "alias3"));
        StringList list2 = StringList.newBuilder().addAllListString(aliases2).build();

        when(manager.getDataProviderMock().getMessageStreams(any(Empty.class))).thenReturn(list1, list2);

        Crawler crawler = manager.createCrawler();

        Iterator<StreamResponse> iterator1 = new MessageSearchResponse(messages1).iterator();
        Iterator<StreamResponse> iterator2 = new MessageSearchResponse(messages2).iterator();

        when(manager.getDataProviderMock().searchMessages(argThat(argument -> {
            List<String> aliases = argument.getStream().getListStringList();

            return aliases.containsAll(List.of("alias1", "alias2"));
        }))).thenReturn(iterator1);

        doReturn(iterator2).when(manager.getDataProviderMock()).searchMessages(argThat(argument -> {
            List<String> aliases = argument.getStream().getListStringList();

            return aliases.contains("alias3");
        }));

        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.newBuilder().build());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        MessageSearchRequest expectedRequest1 = MessageSearchRequest.newBuilder().setStream(StringList.newBuilder().addAllListString(List.of("alias1", "alias2")).build()).build();
        verify(manager.getDataProviderMock(), times(1)).searchMessages(argThat(actual -> expectedRequest1.getStream().equals(actual.getStream())));

        MessageSearchRequest expectedRequest2 = MessageSearchRequest.newBuilder().setStream(StringList.newBuilder().addListString("alias3").build()).build();
        verify(manager.getDataProviderMock(), times(1)).searchMessages(argThat(actual -> expectedRequest2.getStream().equals(actual.getStream())));

        verify(manager.getDataProviderMock(), times(3)).getMessageStreams(any(Empty.class));

        MessageDataRequest expected1 = createExpectedMessageDataRequest(messages1);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected1.getMessageDataList().equals(actual.getMessageDataList())));

        MessageDataRequest expected2 = createExpectedMessageDataRequest(messages2);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected2.getMessageDataList().equals(actual.getMessageDataList())));
    }
}
