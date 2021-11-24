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

package com.exactpro.th2.crawler.messages.strategy;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.AbstractStrategy;
import com.exactpro.th2.crawler.Action;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.InternalInterval;
import com.exactpro.th2.crawler.Report;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageIDs;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics.ProcessorMethod;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.protobuf.Timestamp;

public class MessagesStrategy extends AbstractStrategy<MessagesCrawlerData, ResumeMessageIDs> {
    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;
    public static final BinaryOperator<MessageID> EARLIEST_SEQUENCE = (first, second) -> first.getSequence() > second.getSequence() ? second : first;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagesStrategy.class);
    private final DataProviderService provider;
    private final CrawlerMetrics metrics;
    private final CrawlerConfiguration config;

    public MessagesStrategy(
            @NotNull DataProviderService provider,
            @NotNull CrawlerMetrics metrics,
            @NotNull CrawlerConfiguration config
    ) {
        super(metrics);
        this.provider = requireNonNull(provider, "'Provider' parameter");
        this.metrics = requireNonNull(metrics, "'Metrics' parameter");
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
    public MessagesCrawlerData requestData(@NotNull Timestamp start, @NotNull Timestamp end, @NotNull DataParameters parameters,
                                           @Nullable ResumeMessageIDs continuation) {
        requireNonNull(start, "'start' parameter");
        requireNonNull(end, "'end' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        Map<StreamKey, MessageID> resumeIds = continuation == null ? null : continuation.getIds();
        Map<StreamKey, MessageID> startIDs = resumeIds == null ? initialStartIds(start, parameters.getSessionAliases()) : resumeIds;
        int batchSize = config.getBatchSize();
        MessagesSearchParameters searchParams = MessagesSearchParameters.builder()
                .setFrom(start)
                .setTo(end)
                .setBatchSize(batchSize)
                .setResumeIds(resumeIds)
                .setAliases(parameters.getSessionAliases())
                .build();
        SearchResult<MessageData> result = CrawlerUtils.searchMessages(provider, searchParams, metrics);
        List<MessageData> messages = result.getData();

        resumeIds = new HashMap<>(resumeIds == null ? Collections.emptyMap() : resumeIds);
        Map<StreamKey, MessageID> nextResumeIds = requireNonNull(result.getStreamsInfo(), "response from data provider does not have the StreamsInfo")
                .getStreamsList()
                .stream()
                .collect(toUnmodifiableMap(
                        stream -> new StreamKey(stream.getSession(), stream.getDirection()),
                        Stream::getLastId,
                        BinaryOperator.maxBy(Comparator.comparingLong(MessageID::getSequence))
                ));

        putAndCheck(nextResumeIds, resumeIds, "computing resume IDs");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("New resume ids: {}", resumeIds.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + MessageUtils.toJson(entry.getValue()))
                    .collect(Collectors.joining(",")));
        }
        return new MessagesCrawlerData(
                messages,
                new ResumeMessageIDs(startIDs, nextResumeIds),
                messages.size() == batchSize
        );
    }

    @NotNull
    @Override
    public Report<ResumeMessageIDs> processData(
            @NotNull DataProcessorService processor,
            @NotNull InternalInterval interval,
            @NotNull DataParameters parameters,
            @NotNull MessagesCrawlerData data
    ) {
        requireNonNull(processor, "'processor' parameter");
        requireNonNull(interval, "'interval' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        requireNonNull(data, "'data' parameter");

        CrawlerId crawlerId = parameters.getCrawlerId();
        Interval original = interval.getOriginal();

        List<MessageData> messages = data.getData();

        if (messages.isEmpty()) {
            LOGGER.info("No more messages in interval from: {}, to: {}", original.getStartTime(), original.getEndTime());
            return Report.empty();
        }

        MessageDataRequest messageRequest = MessageDataRequest.newBuilder().setId(crawlerId).addAllMessageData(messages).build();

        MessageResponse response = sendMessagesToProcessor(processor, messageRequest);

        if (response.hasStatus()) {
            if (response.getStatus().getHandshakeRequired()) {
                return Report.handshake();
            }
        }

        List<MessageID> responseIds = response.getIdsList();
        Entry<Integer, Map<StreamKey, MessageID>> processedResult = processServiceResponse(responseIds, messages);

        int processedMessagesCount = processedResult == null ? messages.size() : processedResult.getKey();
        long remaining = messages.size() - processedMessagesCount;

        ResumeMessageIDs continuation = null;
        if (response.getIdsCount() > 0) {
            requireNonNull(processedResult, () -> "processServiceResponse cannot be null for not empty IDs in response: " + responseIds);
            Map<StreamKey, MessageID> checkpoints = processedResult.getValue();

            if (!checkpoints.isEmpty()) {

                ResumeMessageIDs previous = data.getContinuation();
                Map<StreamKey, MessageID> grouped = new HashMap<>(previous.getStartIDs());
                putAndCheck(checkpoints, grouped, "creating continuation from processor response");
                continuation = new ResumeMessageIDs(previous.getStartIDs(), grouped);
            }
        }

        return new Report<>(Action.CONTINUE, processedMessagesCount, remaining, continuation);
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
        MessageResponse response = dataProcessor.sendMessage(messageRequest);
        metrics.processorMethodInvoked(ProcessorMethod.SEND_MESSAGE);
        return response;
    }

    private void putAndCheck(
            Map<StreamKey, MessageID> transferFrom,
            Map<StreamKey, MessageID> transferTo,
            String action
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
                    LOGGER.warn("The new checkpoint ID {} has less sequence than the previous one {} for stream key {} when {}",
                            innerMessageId.getSequence(), prevInnerMessageId.getSequence(), key, action);
                }
                return prevSeqHigher ? prevInnerMessageId : innerMessageId;
            });
        }
    }

    private Map<StreamKey, MessageID> initialStartIds(Timestamp fromTimestamp, Collection<String> aliases) {
        int batchSize = 1;
        var parameters = MessagesSearchParameters.builder()
                .setFrom(fromTimestamp)
                .setTo(fromTimestamp)
                .setBatchSize(batchSize)
                .setAliases(aliases)
                .build();
        SearchResult<MessageData> searchResult = CrawlerUtils.searchMessages(provider, parameters, metrics);
        SearchResult<MessageData> oppositeRequest = null;

        if (!searchResult.getData().isEmpty()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Initialising start IDs request has returned unexpected data: " + searchResult.getData().stream()
                        .map(MessageUtils::toJson).collect(Collectors.joining(", ")));
                // We have a message with timestamp equal the `fromTimestamp`
                // Because of that we need to make a request in opposite direction
                // and select the first message for each pair alias + direction that were in the response (should be a single message)
                oppositeRequest = CrawlerUtils.searchMessages(provider, MessagesSearchParameters.builder()
                        .setFrom(fromTimestamp)
                        .setBatchSize(batchSize)
                        .setResumeIds(associateWithStreamKey(searchResult.getData().stream().map(MessageData::getMessageId), EARLIEST_SEQUENCE))
                        .setAliases(aliases)
                        .setTimeRelation(TimeRelation.PREVIOUS)
                        .build(), metrics);
            }
        }
        return concat(
                requireNonNull(searchResult.getStreamsInfo(), "stream info is null for initial start IDs response")
                        .getStreamsList().stream()
                        .map(Stream::getLastId),
                oppositeRequest == null ? empty() : oppositeRequest.getData().stream().map(MessageData::getMessageId)
        ).collect(toUnmodifiableMap(
                this::createStreamKeyFrom,
                Function.identity(),
                LATEST_SEQUENCE
        ));
    }

    private @Nullable Entry<@NotNull Integer, @NotNull Map<StreamKey, MessageID>> processServiceResponse(
            List<MessageID> responseIds,
            List<MessageData> messages
    ) {
        if (responseIds.isEmpty()) {
            return null;
        }
        Map<StreamKey, MessageID> checkpointByDirection = associateWithStreamKey(responseIds.stream(), LATEST_SEQUENCE);

        int messageCount = 0;
        var skipAliases = new HashSet<String>(responseIds.size());
        for (MessageData data : messages) {
            MessageID messageId = data.getMessageId();
            String sessionAlias = messageId.getConnectionId().getSessionAlias();
            // Update the last message for alias + direction
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
    private Map<StreamKey, MessageID> associateWithStreamKey(java.util.stream.Stream<MessageID> stream, BinaryOperator<MessageID> mergeFunction) {
        return stream.collect(toMap(
                this::createStreamKeyFrom,
                Function.identity(),
                mergeFunction
        ));
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
    private StreamKey createStreamKeyFrom(MessageID messageID) {
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
}
