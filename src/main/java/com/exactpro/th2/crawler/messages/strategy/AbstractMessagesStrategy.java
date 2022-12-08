/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.messages.strategy;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.AbstractStrategy;
import com.exactpro.th2.crawler.Action;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.DataType;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.Report;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageIDs;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.state.v2.InnerMessageId;
import com.exactpro.th2.crawler.state.v2.RecoveryState;
import com.exactpro.th2.crawler.state.v2.StreamKey;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public abstract class AbstractMessagesStrategy extends AbstractStrategy<MessagesCrawlerData.ResumeMessageIDs, MessagesCrawlerData.MessagePart> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessagesStrategy.class);

    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;
    public static final BinaryOperator<MessageID> EARLIEST_SEQUENCE = (first, second) -> first.getSequence() > second.getSequence() ? second : first;

    protected final DataProviderService provider;
    protected final CrawlerConfiguration config;

    public AbstractMessagesStrategy(
            @NotNull DataProviderService provider,
            @NotNull CrawlerMetrics metrics,
            @NotNull CrawlerConfiguration config
    ) {
        super(metrics);
        this.provider = requireNonNull(provider, "'Provider' parameter");
        this.config = requireNonNull(config, "'Config' parameter");
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
    public MessagesCrawlerData.ResumeMessageIDs continuationFromState(@NotNull RecoveryState state) {
        requireNonNull(state, "'state' parameter");
        Map<StreamKey, InnerMessageId> ids = requireNonNullElse(state.getLastProcessedMessages(), Map.of());
        return new MessagesCrawlerData.ResumeMessageIDs(Map.of(), toMessageIDs(ids));
    }

    @NotNull
    @Override
    public RecoveryState continuationToState(@Nullable RecoveryState current, @NotNull MessagesCrawlerData.ResumeMessageIDs continuation, long processedData) {
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
    public Report<MessagesCrawlerData.ResumeMessageIDs> processData(
            @NotNull DataProcessorService processor,
            @NotNull InternalInterval interval,
            @NotNull DataParameters parameters,
            @NotNull MessagesCrawlerData.MessagePart data,
            @Nullable MessagesCrawlerData.ResumeMessageIDs prevCheckpoint) {
        requireNonNull(processor, "'processor' parameter");
        requireNonNull(interval, "'interval' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        requireNonNull(data, "'data' parameter");

        Interval original = interval.getOriginal();

        MessageDataRequest request = data.getRequest();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Sending request with messages: {}", request.getMessageDataList().stream()
                    .map(AbstractMessagesStrategy::formatMessageId)
                    .collect(Collectors.joining(",")));
        }
        List<MessageGroupResponse> messages = request.getMessageDataList();

        if (messages.isEmpty()) {
            LOGGER.info("No more messages in interval from: {}, to: {}", original.getStart(), original.getEnd());
            return Report.empty();
        }

        // TODO: Extract to separate method

        MessageResponse response = sendMessagesToProcessor(processor, request);

        if (response.hasStatus()) {
            if (response.getStatus().getHandshakeRequired()) {
                return Report.handshake();
            }
        }

        List<MessageID> responseIds = response.getIdsList();

        Map.Entry<Integer, Map<StreamKey, MessageID>> processedResult = processServiceResponse(responseIds, messages);

        int processedMessagesCount = processedResult == null ? messages.size() : processedResult.getKey();
        long remaining = messages.size() - processedMessagesCount;

        MessagesCrawlerData.ResumeMessageIDs continuation = null;
        if (!responseIds.isEmpty()) {
            requireNonNull(processedResult, () -> "processServiceResponse cannot be null for not empty IDs in response: " + responseIds);
            Map<StreamKey, MessageID> checkpoints = processedResult.getValue();

            if (!checkpoints.isEmpty()) {

                Map<StreamKey, MessageID> grouped = new HashMap<>(prevCheckpoint == null ? data.getStartIDs() : prevCheckpoint.getIds());
                putAndCheck(checkpoints, grouped, "creating continuation from processor response");
                continuation = new MessagesCrawlerData.ResumeMessageIDs(data.getStartIDs(), grouped);
            }
        }

        return new Report<>(Action.CONTINUE, processedMessagesCount, remaining, continuation);
    }

    protected @Nullable Map.Entry<@NotNull Integer, @NotNull Map<StreamKey, MessageID>> processServiceResponse(
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

    protected MessageResponse sendMessagesToProcessor(DataProcessorService dataProcessor, MessageDataRequest messageRequest) {
        long start = System.currentTimeMillis();
        LOGGER.info("Sending request to processor");
        MessageResponse response;
        try {
            response = metrics.measureTime(DataType.MESSAGES, CrawlerMetrics.Method.PROCESS_DATA, () -> dataProcessor.sendMessage(messageRequest));
        } finally {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Received response from processor in {} mls", System.currentTimeMillis() - start);
            }
        }
        metrics.processorMethodInvoked(CrawlerMetrics.ProcessorMethod.SEND_MESSAGE);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Response from processor: " + MessageUtils.toJson(response));
        }
        return response;
    }

    @NotNull
    protected Map<StreamKey, MessageID> toMessageIDs(Map<StreamKey, InnerMessageId> ids) {
        return ids.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        this::toMessageId
                ));
    }

    @NotNull
    protected MessageID toMessageId(Map.Entry<StreamKey, InnerMessageId> entry) {
        var streamKey = entry.getKey();
        var innerId = entry.getValue();
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(streamKey.getSessionAlias()))
                .setDirection(streamKey.getDirection())
                .setSequence(innerId.getSequence())
                .build();
    }

    @NotNull
    protected Map<StreamKey, InnerMessageId> toInnerMessageIDs(Map<StreamKey, MessageID> checkpointByDirection) {
        return checkpointByDirection.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        it -> fromMessageInfo(it.getValue())
                ));
    }

    @NotNull
    public static StreamKey createStreamKeyFrom(MessageID messageID) {
        return new StreamKey(messageID.getBookName(), messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
    }

    @NotNull
    public static Map<StreamKey, MessageID> associateWithStreamKey(Stream<MessageID> stream, BinaryOperator<MessageID> mergeFunction) {
        return stream.collect(toMap(
                MessagesStrategy::createStreamKeyFrom,
                Function.identity(),
                mergeFunction
        ));
    }

    @NotNull
    static Map<StreamKey, MessageID> collectResumeIDs(MessageStreamPointers streamsInfo) {
        return streamsInfo.getMessageStreamPointerList().stream()
                .collect(toUnmodifiableMap(
                        //FIXME: add book id into MessageStream
                        stream -> keyFroMessageStream(stream.getLastId().getBookName(), stream.getMessageStream()),
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
        for (Map.Entry<StreamKey, MessageID> entry : transferFrom.entrySet()) {
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

    protected static void putAndCheck(
            Map<StreamKey, MessageID> transferFrom,
            Map<StreamKey, MessageID> transferTo,
            String action
    ) {
        putAndCheck(transferFrom, transferTo, action, LOGGER);
    }

    private static InnerMessageId fromMessageInfo(MessageID messageID) {
        return new InnerMessageId(
                toInstant(messageID.getTimestamp()),
                messageID.getSequence()
        );
    }

    private static StreamKey keyFroMessageStream(String book, MessageStream stream) {
        return new StreamKey(book, stream.getName(), stream.getDirection()); //FIXME: add book id into MessageStream
    }

    private static String formatMessageId(MessageGroupResponse msg) {
        MessageID id = msg.getMessageId();
        return id.getConnectionId().getSessionAlias() + ":" + id.getDirection() + ":" + id.getSequence();
    }
}
