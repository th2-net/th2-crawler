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

package com.exactpro.th2.crawler.messages.strategy.load;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.AbstractStrategy;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.CrawlerData;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.Report;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageIDs;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.filters.NameFilter;
import com.exactpro.th2.crawler.handler.ProcessorObserver;
import com.exactpro.th2.crawler.messages.strategy.load.MessagesCrawlerData.MessagePart;
import com.exactpro.th2.crawler.messages.strategy.load.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.Method;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsRequest;
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsResponse;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.Group;
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.grpc.MessageStream;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointers;
import com.google.protobuf.Timestamp;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MessagesStrategy extends AbstractStrategy<ResumeMessageIDs, MessagePart> {
    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;
    public static final BinaryOperator<MessageID> EARLIEST_SEQUENCE = (first, second) -> first.getSequence() > second.getSequence() ? second : first;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagesStrategy.class);

    private final DataProviderService provider;
    private final CrawlerMetrics metrics;

    public MessagesStrategy(
            @NotNull DataProviderService provider,
            @NotNull CrawlerMetrics metrics,
            @NotNull CrawlerConfiguration config
    ) {
        super(metrics, config);
        this.provider = requireNonNull(provider, "'Provider' parameter");
        this.metrics = requireNonNull(metrics, "'Metrics' parameter");
    }

    @Override
    public void setupIntervalInfo(@NotNull IntervalInfo.Builder info, @Nullable RecoveryState state) {
        requireNonNull(info, "'info' parameter");
        Map<StreamKey, InnerMessageId> lastProcessedMessages = state == null ? null : state.getLastProcessedMessages();
        if (lastProcessedMessages != null && !lastProcessedMessages.isEmpty()) {
            info.setLastMessageIds(MessageIDs.newBuilder()
                    .addAllMessageIds(lastProcessedMessages.entrySet().stream()
                            .map(this::toMessageId)
                            .collect(Collectors.toList()))
                    .build());
        }
    }

    @NotNull
    @Override
    public ResumeMessageIDs continuationFromState(@NotNull RecoveryState state) {
        requireNonNull(state, "'state' parameter");
        Map<StreamKey, InnerMessageId> ids = requireNonNullElse(state.getLastProcessedMessages(), Map.of());
        return new ResumeMessageIDs(Map.of(), toMessageIDs(ids));
    }

    @NotNull
    @Override
    public RecoveryState continuationToState(@Nullable RecoveryState current, @NotNull ResumeMessageIDs continuation, long processedData) {
        requireNonNull(continuation, "'continuation' parameter");
        if (current == null) {
            Map<StreamKey, MessageID> startIntervalIDs = new HashMap<>(continuation.getStartIDs());
            putAndCheck(continuation.getIds(), startIntervalIDs, "creating state from continuation");

            return new RecoveryState(null, toInnerMessageIDs(startIntervalIDs), 0, processedData);
        }
        Map<StreamKey, InnerMessageId> old = current.getLastProcessedMessages();
        Map<StreamKey, MessageID> lastProcessedMessages = new HashMap<>(old == null ? continuation.getIds() : toMessageIDs(old));
        putAndCheck(continuation.getIds(), lastProcessedMessages, "creating state from current state and continuation");

        return new RecoveryState(current.getLastProcessedEvent(), toInnerMessageIDs(lastProcessedMessages),
                current.getLastNumberOfEvents(),
                processedData);
    }

    @NotNull
    @Override
    public CrawlerData<ResumeMessageIDs, MessagePart> requestData(@NotNull Timestamp start, @NotNull Timestamp end, @NotNull DataParameters parameters,
                                                                  @Nullable ResumeMessageIDs continuation) {
        requireNonNull(start, "'start' parameter");
        requireNonNull(end, "'end' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        Map<StreamKey, MessageID> resumeIds = continuation == null ? null : continuation.getIds();
        Map<StreamKey, MessageID> startIDs = resumeIds == null ? initialStartIds(parameters.getSessionAliases()) : resumeIds;
        MessagesSearchParameters searchParams = MessagesSearchParameters.builder()
                .setFrom(start)
                .setTo(end)
                .setResumeIds(resumeIds)
                .setAliases(parameters.getSessionAliases())
                .setUseGroups(config.getUseGroupsForRequest())
                .build();

        NameFilter filter = config.getFilter();
        return new MessagesCrawlerData(
                metrics,
                config,
                CrawlerUtils.loadMessages(provider, searchParams, metrics),
                startIDs,
                parameters.getCrawlerId(),
                msg -> filter == null || filter.accept(msg.getMetadata().getMessageType())
        );
    }

    @NotNull
    @Override
    public Report<ResumeMessageIDs> processData(
            @NotNull DataProcessorService processor,
            @NotNull InternalInterval interval,
            @NotNull DataParameters parameters,
            @NotNull MessagePart data,
            @Nullable ResumeMessageIDs prevCheckpoint) {
        return super.processData(processor, interval, parameters, data, prevCheckpoint);
    }

    @NotNull
    private MessageID toMessageId(Entry<StreamKey, InnerMessageId> entry) {
        var streamKey = entry.getKey();
        var innerId = entry.getValue();
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(streamKey.getSessionAlias()))
                .setDirection(streamKey.getDirection())
                .setSequence(innerId.getSequence())
                .build();
    }

    private MessageResponse sendMessagesToProcessor(DataProcessorService dataProcessor, MessageDataRequest messageRequest) {
        long start = System.currentTimeMillis();
        LOGGER.info("Sending request to processor");
        MessageResponse response;
        try {
            if (config.getDebug().getEnableProcessor()) {
                response = metrics.measureTime(DataType.MESSAGES, Method.PROCESS_DATA, () -> dataProcessor.sendMessage(messageRequest));
            } else {
                response = MessageResponse.getDefaultInstance();
            }
        } finally {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Received response from processor in {} mls", System.currentTimeMillis() - start);
            }
        }
        metrics.processorMethodInvoked(ProcessorMethod.SEND_MESSAGE);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Response from processor: " + MessageUtils.toJson(response));
        }
        return response;
    }

    @NotNull
    static Map<StreamKey, MessageID> collectResumeIDs(MessageStreamPointers streamsInfo) {
        return streamsInfo.getMessageStreamPointerList().stream()
                .collect(toUnmodifiableMap(
                        stream -> keyFroMessageStream(stream.getMessageStream()),
                        MessageStreamPointer::getLastId,
                        BinaryOperator.maxBy(Comparator.comparingLong(MessageID::getSequence))
                ));
    }

    static void putAndCheck(
            Map<StreamKey, MessageID> transferFrom,
            Map<StreamKey, MessageID> transferTo,
            String action,
            Logger logger
    ) {
        for (Entry<StreamKey, MessageID> entry : transferFrom.entrySet()) {
            var streamKey = entry.getKey();
            var innerMessageId = entry.getValue();
            transferTo.compute(streamKey, (key, prevInnerMessageId) -> {
                if (prevInnerMessageId == null) {
                    return innerMessageId;
                }
                boolean prevSeqHigher = prevInnerMessageId.getSequence() > innerMessageId.getSequence();
                if (prevSeqHigher) {
                    logger.warn("The new checkpoint ID {} has less sequence than the previous one {} for stream key {} when {}",
                            innerMessageId.getSequence(), prevInnerMessageId.getSequence(), key, action);
                    return prevInnerMessageId;
                }
                return innerMessageId;
            });
        }
    }

    private static void putAndCheck(
            Map<StreamKey, MessageID> transferFrom,
            Map<StreamKey, MessageID> transferTo,
            String action
    ) {
        putAndCheck(transferFrom, transferTo, action, LOGGER);
    }

    private Map<StreamKey, MessageID> initialStartIds(Collection<String> aliases) {
        Map<StreamKey, MessageID> ids = new HashMap<>(aliases.size() * 2);
        for (String alias : aliases) {
            ConnectionID connectionID = ConnectionID.newBuilder().setSessionAlias(alias).build();
            Consumer<Direction> addIdAction = direction -> ids.put(new StreamKey(alias, direction), createMessageId(connectionID, direction));
            addIdAction.accept(Direction.FIRST);
            addIdAction.accept(Direction.SECOND);
        }

        return ids;
    }

    @NotNull
    private MessageID createMessageId(ConnectionID connectionID, Direction direction) {
        return MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(-1)
                .setConnectionId(connectionID)
                .build();
    }

    private static StreamKey keyFroMessageStream(MessageStream stream) {
        return new StreamKey(stream.getName(), stream.getDirection());
    }

    private @NotNull Pair<List<MessageGroupResponse>, MessageStreamPointers> collect(Iterator<MessageSearchResponse> responses) {
        List<MessageGroupResponse> data = null;
        MessageStreamPointers info = null;

        while (responses.hasNext()) {
            MessageSearchResponse next = responses.next();
            if (next.hasMessage()) {
                if (data == null) {
                    data = new ArrayList<>();
                }
                data.add(next.getMessage());
                continue;
            }
            if (next.hasMessageStreamPointers()) {
                info = next.getMessageStreamPointers();
            }
        }
        return new Pair<>(
                data == null ? Collections.emptyList() : data,
                requireNonNull(info, "stream info is null for initial start IDs response")
        );
    }

    private @Nullable Entry<@NotNull Integer, @NotNull Map<StreamKey, MessageID>> processServiceResponse(
            List<MessageID> responseIds,
            List<MessageGroupResponse> messages
    ) {
        if (responseIds.isEmpty()) {
            return null;
        }
        Map<StreamKey, MessageID> checkpointByDirection = associateWithStreamKey(responseIds.stream(), LATEST_SEQUENCE);

        int messageCount = 0;
        var skipAliases = new HashSet<String>(responseIds.size());
        for (MessageGroupResponse data : messages) {
            MessageID messageId = data.getMessageId();
            String sessionAlias = messageId.getConnectionId().getSessionAlias();
            // Update the last message for alias + direction
            // FIXME: the update metric implementation one by one is heavy to much
            metrics.lastMessage(sessionAlias, messageId.getDirection(), data);
            if (skipAliases.contains(sessionAlias)) {
                continue;
            }
            messageCount++;
            MessageID checkpointId = checkpointByDirection.get(createStreamKeyFrom(messageId));
            if (messageId.equals(checkpointId)) {
                skipAliases.add(sessionAlias);
            }
        }

        return Map.entry(messageCount, checkpointByDirection);
    }

    @NotNull
    public static Map<StreamKey, MessageID> associateWithStreamKey(Stream<MessageID> stream, BinaryOperator<MessageID> mergeFunction) {
        return stream.collect(toMap(
                MessagesStrategy::createStreamKeyFrom,
                Function.identity(),
                mergeFunction
        ));
    }

    @Override
    public void loadData(ProcessorObserver observer,
                         @NotNull Timestamp start,
                         @NotNull Timestamp end,
                         @NotNull DataParameters parameters) {
        CradleMessageGroupsRequest.Builder builder = CradleMessageGroupsRequest.newBuilder()
                        .setStartTimestamp(start)
                        .setEndTimestamp(end)
                        .setRoutingPropertyName(config.getRoutingPropertyName())
                        .setRoutingPropertyValue(config.getRoutingPropertyValue());

        parameters.getSessionAliases().stream()
                .map(groupName -> Group.newBuilder().setName(groupName).build())
                        .forEach(builder::addMessageGroup);

        CradleMessageGroupsResponse info = provider.loadCradleMessageGroups(builder.build());
        observer.onProviderResponse(info);
    }

    @NotNull
    private Map<StreamKey, InnerMessageId> toInnerMessageIDs(Map<StreamKey, MessageID> checkpointByDirection) {
        return checkpointByDirection.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Entry::getKey,
                        it -> fromMessageInfo(it.getValue())
                ));
    }

    @NotNull
    public static StreamKey createStreamKeyFrom(MessageID messageID) {
        return new StreamKey(messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
    }

    private static InnerMessageId fromMessageInfo(MessageID messageID) {
        return new InnerMessageId(
                null,
                messageID.getSequence()
        );
    }

    @NotNull
    private Map<StreamKey, MessageID> toMessageIDs(Map<StreamKey, InnerMessageId> ids) {
        return ids.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Entry::getKey,
                        this::toMessageId
                ));
    }

    private static String formatMessageId(MessageGroupResponse msg) {
        MessageID id = msg.getMessageId();
        return id.getConnectionId().getSessionAlias() + ":" + id.getDirection() + ":" + id.getSequence();
    }
}