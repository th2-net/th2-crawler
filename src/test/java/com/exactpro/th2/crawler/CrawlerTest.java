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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.util.StorageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.Status;
import com.exactpro.th2.crawler.exception.UnexpectedDataProcessorException;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v2.InnerEventId;
import com.exactpro.th2.crawler.state.v2.InnerMessageId;
import com.exactpro.th2.crawler.state.v2.RecoveryState;
import com.exactpro.th2.crawler.state.v2.StreamKey;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointers;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.crawler.CrawlerManager.BOOK_NAME;
import static com.exactpro.th2.crawler.CrawlerManager.NAME;
import static com.exactpro.th2.crawler.CrawlerManager.VERSION;
import static com.exactpro.th2.crawler.TestUtilKt.createMessageID;
import static com.exactpro.th2.crawler.util.CrawlerUtilKt.maxOrDefault;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrawlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerTest.class);
    private final StateService<RecoveryState> stateService =
            StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);

    @Test
    @DisplayName("Returns correct value as a sleep timeout when no intervals created")
    public void returnsCorrectSleepTime() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant time = Instant.now();
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                time.toString(), DataType.EVENTS, Duration.ofHours(1), toSet(CrawlerManager.SCOPES), Collections.emptySet(), Collections.emptySet(), 0, ChronoUnit.MINUTES);
        CrawlerManager manager = new CrawlerManager(configuration);

        Crawler crawler = manager.createCrawler(() -> time.plus(35, ChronoUnit.MINUTES)); // 25 minutes to the interval
        Duration sleep = crawler.process();

        Assertions.assertEquals(Duration.ofMinutes(25), sleep);

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(BookId.class), any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Returns correct value as a sleep timeout when has intervals")
    public void returnsCorrectSleepTimeWhenHasIntervals() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant time = Instant.now();
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                time.toString(), DataType.EVENTS, Duration.ofHours(1), toSet(CrawlerManager.SCOPES), Collections.emptySet(), Collections.emptySet(), 0, ChronoUnit.MINUTES);
        CrawlerManager manager = new CrawlerManager(configuration);
        Instant end = time.plus(1, ChronoUnit.HOURS);
        manager.getIntervalsWorkerMock().storeInterval(Interval.builder()
                .setStart(time)
                .setEnd(end)
                .setLastUpdate(end)
                .setBookId(new BookId(BOOK_NAME))
                .setCrawlerName(CrawlerManager.NAME)
                .setCrawlerVersion(CrawlerManager.VERSION)
                .setCrawlerType(DataType.EVENTS.getTypeName())
                .setProcessed(true)
                .build());

        Crawler crawler = manager.createCrawler(() -> time.plus(60 + 35, ChronoUnit.MINUTES)); // 25 minutes to the next interval
        Duration sleep = crawler.process();

        Assertions.assertEquals(Duration.ofMinutes(25), sleep);

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(BookId.class), any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Calling method process()")
    public void processMethodCall() throws UnexpectedDataProcessorException, CradleStorageException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, toSet(CrawlerManager.SCOPES), Collections.emptySet(), Collections.emptySet());
        CrawlerManager manager = new CrawlerManager(configuration);

        Crawler crawler = manager.createCrawler();
        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(BookId.class), any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));
        verify(manager.getDataProviderMock()).searchEvents(any(EventSearchRequest.class));
    }

    @Test
    public void testCrawlerMessages() throws UnexpectedDataProcessorException, CradleStorageException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Message> messages = MessageReaderKt.readMessages(Paths.get("src/test/resources/messages.txt"));
        Iterator<MessageSearchResponse> iterator = new MessageSearchResponseAdapter(messages).iterator();
        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class))).thenReturn(iterator);
        when(manager.getDataServiceMock().sendMessage(any(MessageDataRequest.class))).thenReturn(MessageResponse.getDefaultInstance());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(BookId.class), any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
        verify(manager.getIntervalsWorkerMock()).storeInterval(any(Interval.class));

        verify(manager.getDataProviderMock()).searchMessages(any(MessageSearchRequest.class));

        MessageDataRequest expected = createExpectedMessageDataRequest(messages);
        verify(manager.getDataServiceMock()).sendMessage(argThat(actual -> expected.getMessageDataList().equals(actual.getMessageDataList())));
    }

    @Test
    public void startProcessMessages() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.MESSAGES, Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS));
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Message> messages = List.of(
                createMessage(CrawlerManager.SESSIONS[0], Direction.FIRST, 1, from.plusNanos(1)),
                createMessage(CrawlerManager.SESSIONS[0], Direction.SECOND, 2, from.plusNanos(2)),
                createMessage(CrawlerManager.SESSIONS[1], Direction.SECOND, 3, from.plusNanos(3))
        );

        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class)))
                .thenAnswer(invocation ->  new MessageSearchResponseAdapter(messages).iterator());

        crawler.process();

        verify(manager.getDataProviderMock())
                .searchMessages(eq(createMessageSearchRequest(from, to)));

        verify(manager.getIntervalsWorkerMock(), times(2))
                .updateRecoveryState(
                        any(Interval.class),
                        argThat(argument -> createRecoveryState(Collections.emptyList(), messages).equals(stateService.deserialize(argument)))
                );
    }
    @Test
    public void continueProcessMessages() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.MESSAGES, Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS));
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        Collection<Message> messages = List.of(
                createMessage(CrawlerManager.SESSIONS[0], Direction.FIRST, 1, from.plusNanos(1)),
                createMessage(CrawlerManager.SESSIONS[0], Direction.SECOND, 2, from.plusNanos(2)),
                createMessage(CrawlerManager.SESSIONS[1], Direction.SECOND, 3, from.plusNanos(3))
        );

        RecoveryState recoveryState = createRecoveryState(Collections.emptyList(), messages);
        CrawlerManager manager = new CrawlerManager(
                configuration,
                List.of(createInterval(
                        configuration,
                        from,
                        to,
                        stateService.serialize(recoveryState))
                )
        );
        Crawler crawler = manager.createCrawler();

        when(manager.getDataProviderMock().searchMessages(any(MessageSearchRequest.class)))
                .thenAnswer(invocation -> Collections.emptyIterator());

        crawler.process();

        MessageSearchRequest messageSearchRequest = createMessageSearchRequest(
                from,
                to,
                messages.stream()
                        .map(Message::getMetadata)
                        .map(MessageMetadata::getId)
                        .collect(Collectors.toList()));
        verify(manager.getDataProviderMock(), description(messageSearchRequest.toString()))
                .searchMessages(argThat(argument -> isEqual(messageSearchRequest, argument)));

        verify(
                manager.getIntervalsWorkerMock(),
                times(1)
                        .description(stateService.serialize(recoveryState))
        ).updateRecoveryState(
                any(Interval.class),
                argThat(argument -> recoveryState.equals(stateService.deserialize(argument)))
        );
    }
    @Test
    public void startProcessMessageGroups() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.MESSAGES, Collections.emptySet(), toSet(CrawlerManager.GROUPS), Collections.emptySet());
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Message> messages = List.of(
                createMessage(CrawlerManager.SESSIONS[0], Direction.FIRST, 1, from.plusNanos(1)),
                createMessage(CrawlerManager.SESSIONS[0], Direction.SECOND, 2, from.plusNanos(2)),
                createMessage(CrawlerManager.SESSIONS[1], Direction.SECOND, 3, from.plusNanos(3))
        );

        when(manager.getDataProviderMock().searchMessageGroups(any(MessageGroupsSearchRequest.class)))
                .thenAnswer(invocation ->  new MessageSearchResponseAdapter(messages).iterator());

        crawler.process();

        verify(manager.getDataProviderMock())
                .searchMessageGroups(eq(createMessageGroupsSearchRequest(from, to)));

        verify(manager.getIntervalsWorkerMock(), times(2))
                .updateRecoveryState(
                        any(Interval.class),
                        argThat(argument -> createRecoveryState(Collections.emptyList(), messages).equals(stateService.deserialize(argument)))
                );
    }
    @Test
    public void continueProcessMessageGroups() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.MESSAGES, Collections.emptySet(), toSet(CrawlerManager.GROUPS), Collections.emptySet());
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        Collection<Message> messages = List.of(
                createMessage(CrawlerManager.SESSIONS[0], Direction.FIRST, 1, from.plusNanos(1)),
                createMessage(CrawlerManager.SESSIONS[0], Direction.SECOND, 2, from.plusNanos(2)),
                createMessage(CrawlerManager.SESSIONS[1], Direction.SECOND, 3, from.plusNanos(3))
        );

        RecoveryState recoveryState = createRecoveryState(Collections.emptyList(), messages);
        CrawlerManager manager = new CrawlerManager(
                configuration,
                List.of(createInterval(
                        configuration,
                        from,
                        to,
                        stateService.serialize(recoveryState))
                )
        );
        Crawler crawler = manager.createCrawler();


        when(manager.getDataProviderMock().searchMessageGroups(any(MessageGroupsSearchRequest.class)))
                .thenAnswer(invocation -> Collections.emptyIterator());

        crawler.process();

        MessageGroupsSearchRequest messageSearchRequest = createMessageGroupsSearchRequest(
                from,
                to,
                messages.stream()
                        .map(Message::getMetadata)
                        .map(MessageMetadata::getId)
                        .collect(Collectors.toList()));
        verify(manager.getDataProviderMock(), description(messageSearchRequest.toString()))
                .searchMessageGroups(argThat(argument -> isEqual(messageSearchRequest, argument)));

        verify(
                manager.getIntervalsWorkerMock(),
                times(1)
                    .description(stateService.serialize(recoveryState))
        ).updateRecoveryState(
                any(Interval.class),
                argThat(argument -> recoveryState.equals(stateService.deserialize(argument)))
        );
    }
    @Test
    public void startProcessEvents() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.EVENTS, new LinkedHashSet<>(Arrays.asList(CrawlerManager.SCOPES)), Collections.emptySet(), Collections.emptySet());
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        Collection<Event> events = List.of(
                createEvent(CrawlerManager.SCOPES[0], "1", from),
                createEvent(CrawlerManager.SCOPES[0], "2", from.plusNanos(1)),
                createEvent(CrawlerManager.SCOPES[1], "3", from.plusNanos(2))
        );

        when(manager.getDataProviderMock().searchEvents(any(EventSearchRequest.class)))
                .thenAnswer(invocation -> createEventSearchResponse(events).iterator());

        crawler.process();

        verify(manager.getDataProviderMock())
                .searchEvents(eq(createEventSearchRequest(from, to)));

        verify(manager.getIntervalsWorkerMock(), times(2))
                .updateRecoveryState(
                        any(Interval.class),
                        argThat(argument -> createRecoveryState(events, Collections.emptyList()).equals(stateService.deserialize(argument)))
                );
    }
    @Test
    public void continueProcessEvents() throws UnexpectedDataProcessorException, CradleStorageException {
        Instant from = Instant.parse("2021-06-16T12:00:00.00Z");
        CrawlerConfiguration configuration = CrawlerManager
                .createConfig(from.toString(), DataType.EVENTS, new LinkedHashSet<>(Arrays.asList(CrawlerManager.SCOPES)), Collections.emptySet(), Collections.emptySet());
        Instant to = from.plus(Duration.parse(configuration.getDefaultLength()));

        Collection<Event> events = List.of(
                createEvent(CrawlerManager.SCOPES[0], "1", from),
                createEvent(CrawlerManager.SCOPES[0], "2", from.plusNanos(1)),
                createEvent(CrawlerManager.SCOPES[1], "3", from.plusNanos(2))
        );

        RecoveryState recoveryState = createRecoveryState(events, Collections.emptyList());
        CrawlerManager manager = new CrawlerManager(
                configuration,
                List.of(createInterval(
                        configuration,
                        from,
                        to,
                        stateService.serialize(recoveryState))
                )
        );
        Crawler crawler = manager.createCrawler();

        when(manager.getDataProviderMock().searchEvents(any(EventSearchRequest.class)))
                .thenAnswer(invocation -> Collections.emptyIterator());

        crawler.process();

        EventSearchRequest eventSearchRequest = createEventSearchRequest(from, to);
        verify(manager.getDataProviderMock())
                .searchEvents(eq(eventSearchRequest));

        verify(manager.getIntervalsWorkerMock(), description(eventSearchRequest.toString()))
                .updateRecoveryState(
                        any(Interval.class),
                        argThat(argument -> recoveryState.equals(stateService.deserialize(argument)))
                );
    }
    private Interval createInterval(CrawlerConfiguration configuration, Instant from, Instant to, String recoveryState) {
        return Interval.builder()
                .setBookId(new com.exactpro.cradle.BookId(configuration.getBook()))
                .setCrawlerName(NAME)
                .setCrawlerVersion(VERSION)
                .setCrawlerType(configuration.getType().getTypeName())
                .setLastUpdate(Instant.now())
                .setStart(from)
                .setEnd(to)
                .setRecoveryState(recoveryState)
                .build();
    }
    private RecoveryState createRecoveryState(Collection<Event> events, Collection<Message> messages) {

        Optional<InnerEventId> lastEvent = events.stream()
                .map(Event::getId)
                .sorted((eventIdA, eventIdB) -> Timestamps.compare(eventIdB.getStartTimestamp(), eventIdA.getStartTimestamp()) )
                .map(id -> new InnerEventId(id.getBookName(), id.getScope(), StorageUtils.toInstant(id.getStartTimestamp()), id.getId()))
                .findFirst();
        Map<StreamKey, InnerMessageId> processedMessages = messages.stream()
                .map(Message::getMetadata)
                .map(MessageMetadata::getId)
                .collect(
                        Collectors.toMap(
                                id -> new StreamKey(id.getBookName(), id.getConnectionId().getSessionAlias(), id.getDirection()),
                                id -> new InnerMessageId(StorageUtils.toInstant(id.getTimestamp()), id.getSequence())
                        )
                );
        RecoveryState recoveryState = new RecoveryState(
                lastEvent.orElse(null),
                processedMessages.isEmpty() ? null : processedMessages,
                events.size(),
                messages.size()
        );
        LOGGER.info("Created recovery state {}", recoveryState);

        return recoveryState;
    }
    private MessageSearchRequest createMessageSearchRequest(Instant from, Instant to, List<MessageID> recoveryIds) {
        MessageSearchRequest.Builder builder = MessageSearchRequest.newBuilder()
                .setStartTimestamp(toTimestamp(from))
                .setEndTimestamp(toTimestamp(to));
        builder.getBookIdBuilder().setName(CrawlerManager.BOOK_NAME);
        for (String session : CrawlerManager.SESSIONS) {
            builder.addStreamBuilder().setName(session).setDirection(Direction.FIRST);
            builder.addStreamBuilder().setName(session).setDirection(Direction.SECOND);
        }
        for (MessageID recoveryId : recoveryIds) {
            MessageStreamPointer.Builder streamPointerBuilder = builder.addStreamPointerBuilder();
            streamPointerBuilder.getMessageStreamBuilder()
                    .setName(recoveryId.getConnectionId().getSessionAlias())
                    .setDirection(recoveryId.getDirection());
            streamPointerBuilder.getLastIdBuilder()
                    .setConnectionId(recoveryId.getConnectionId())
                    .setDirection(recoveryId.getDirection())
                    .setTimestamp(recoveryId.getTimestamp())
                    .setSequence(recoveryId.getSequence());
        }
        return builder.build();
    }
    private MessageSearchRequest createMessageSearchRequest(Instant from, Instant to) {
        return createMessageSearchRequest(from, to, Collections.emptyList());
    }
    private MessageGroupsSearchRequest createMessageGroupsSearchRequest(Instant from, Instant to, List<MessageID> recoveryIds) {
        Timestamp resultFrom = maxOrDefault(recoveryIds.stream().map(MessageID::getTimestamp), toTimestamp(from));

        MessageGroupsSearchRequest.Builder builder = MessageGroupsSearchRequest.newBuilder()
                .setStartTimestamp(resultFrom)
                .setEndTimestamp(toTimestamp(to));
        builder.getSortBuilder().setValue(true);
        builder.getBookIdBuilder().setName(CrawlerManager.BOOK_NAME);
        for (String group : CrawlerManager.GROUPS) {
            builder.addMessageGroupBuilder().setName(group);
        }
        return builder.build();
    }
    private MessageGroupsSearchRequest createMessageGroupsSearchRequest(Instant from, Instant to) {
        return createMessageGroupsSearchRequest(from, to, Collections.emptyList());
    }
    private Message createMessage(String sessionAlias, Direction direction, long sequence, Instant timestamp) {
        Message.Builder builder = MessageUtils.message(CrawlerManager.BOOK_NAME, "msg", direction, sessionAlias);
        builder.getMetadataBuilder()
                .getIdBuilder()
                .setSequence(sequence)
                .setTimestamp(toTimestamp(timestamp));
        return builder.build();
    }
    private boolean isEqual(MessageSearchRequest requestA, MessageSearchRequest requestB) {
        return requestA.getBookId().equals(requestB.getBookId())
                && requestA.getStartTimestamp().equals(requestB.getStartTimestamp())
                && requestA.getEndTimestamp().equals(requestB.getEndTimestamp())
                && requestA.getStreamList().containsAll(requestB.getStreamList())
                && requestA.getStreamPointerList().containsAll(requestB.getStreamPointerList());
    }
    private boolean isEqual(MessageGroupsSearchRequest requestA, MessageGroupsSearchRequest requestB) {
        return requestA.getBookId().equals(requestB.getBookId())
                && requestA.getStartTimestamp().equals(requestB.getStartTimestamp())
                && requestA.getEndTimestamp().equals(requestB.getEndTimestamp())
                && requestA.getMessageGroupList().containsAll(requestB.getMessageGroupList());
    }
    private EventSearchRequest createEventSearchRequest(Instant from, Instant to) {
        EventSearchRequest.Builder builder = EventSearchRequest.newBuilder()
                .setStartTimestamp(toTimestamp(from))
                .setEndTimestamp(toTimestamp(to));
        builder.getBookIdBuilder().setName(CrawlerManager.BOOK_NAME);
        builder.getScopeBuilder().setName(CrawlerManager.SCOPES[0]); // FIXME: handle several scopes
        return builder.build();
    }
    private List<EventSearchResponse> createEventSearchResponse(Collection<Event> events) {
        return events.stream()
                .map(event -> {
                    EventSearchResponse.Builder builder = EventSearchResponse.newBuilder();
                    builder.getEventBuilder()
                            .setEventId(event.getId());
                    return builder.build();
                }).collect(Collectors.toList());

    }
    private Event createEvent(String scope, String id, Instant timestamp) {
        Event.Builder builder = Event.newBuilder();
        builder.getIdBuilder()
                .setBookName(CrawlerManager.BOOK_NAME)
                .setScope(scope)
                .setStartTimestamp(toTimestamp(timestamp))
                .setId(id);
        return builder.build();
    }

    @Test
    public void testCrawlerMessagesMaxOutgoingMessageSize() throws UnexpectedDataProcessorException, CradleStorageException {
        Message prototype = MessageReaderKt.parseMessage("{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"alias1\"},\"direction\":\"SECOND\",\"sequence\":\"1635664585511283004\",\"timestamp\":\"2021-10-31T07:18:18.085342Z\",\"bookName\":\"book\"},\"messageType\":\"reqType\",\"protocol\":\"protocol\"}}");
        MessageGroupResponse prototypeResp = MessageSearchResponseAdapter.createMessageGroupResponse(prototype).build();

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Duration.ofHours(1), Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS), 5, ChronoUnit.MINUTES, prototypeResp.getSerializedSize() * 3);

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
                .thenReturn(MessageResponse.newBuilder() // This code is required for internal state verification
                                .addIds(createMessageID("alias1", Direction.SECOND, 1))
                                .build(),
                        MessageResponse.newBuilder()
                                .addIds(createMessageID("alias1", Direction.SECOND, 3))
                                .build());

        crawler.process();

        verify(manager.getIntervalsWorkerMock()).getIntervals(any(BookId.class), any(Instant.class), any(Instant.class), anyString(), anyString(), anyString());
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
    public void testCrawlerMessagesMaxOutgoingMessageSizeExceeded() throws UnexpectedDataProcessorException, CradleStorageException {
        Message prototype = MessageReaderKt.parseMessage("{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"alias1\"},\"direction\":\"SECOND\",\"sequence\":\"1635664585511283004\",\"timestamp\":\"2021-10-31T07:18:18.085342Z\"},\"messageType\":\"reqType\",\"protocol\":\"protocol\"}}");

        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Duration.ofHours(1), Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS), 5, ChronoUnit.MINUTES, prototype.getSerializedSize() * 2);

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
    public void handshakeNeededAnother() throws UnexpectedDataProcessorException, CradleStorageException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.EVENTS, toSet(CrawlerManager.SCOPES), Collections.emptySet(), Collections.emptySet());

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        when(manager.getDataServiceMock().crawlerConnect(any(CrawlerInfo.class)))
                .thenReturn(DataProcessorInfo.newBuilder().setName("another_crawler").setVersion(CrawlerManager.VERSION).build());

        when(manager.getDataServiceMock().sendEvent(any(EventDataRequest.class))).then(invocation -> {
            EventDataRequest request = invocation.getArgument(0);

            List<com.exactpro.th2.dataprovider.lw.grpc.EventResponse> events = request.getEventDataList();

            EventID eventID = events.get(events.size() - 1).getEventId();

            return EventResponse.newBuilder().setId(eventID).setStatus(Status.newBuilder().setHandshakeRequired(true).build()).build();
        });

        Assertions.assertThrows(UnexpectedDataProcessorException.class, crawler::process);
    }

    @Test
    @DisplayName("Crawler's actions when a data service fails")
    public void dataServiceFail() throws UnexpectedDataProcessorException, CradleStorageException {
        CrawlerConfiguration configuration = CrawlerManager.createConfig(
                "2021-06-16T12:00:00.00Z", DataType.MESSAGES, Collections.emptySet(), Collections.emptySet(), toSet(CrawlerManager.SESSIONS));

        CrawlerManager manager = new CrawlerManager(configuration);
        Crawler crawler = manager.createCrawler();

        String exceptionMessage = "Test exception";

        Message.Builder responseMessage = MessageUtils.message(BOOK_NAME, "test", Direction.FIRST, "alias1");
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

    private Set<String> toSet(String[] array) {
        return new LinkedHashSet<>(Arrays.asList(array));
    }
}
