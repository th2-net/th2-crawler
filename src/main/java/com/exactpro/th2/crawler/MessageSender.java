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

import static com.exactpro.th2.crawler.Crawler.EARLIEST_SEQUENCE;
import static com.exactpro.th2.crawler.Crawler.LATEST_SEQUENCE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.crawler.util.SearchResult;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.TimeRelation;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);
    private final CrawlerConfiguration configuration;
    private final int batchSize;
    private final CrawlerId crawlerId;
    private final DataProcessorService dataProcessor;
    private final DataProviderService dataProviderService;
    private final IntervalsWorker intervalsWorker;
    private final StateService<RecoveryState> stateService;
    private final Handshaker handshaker;
    private final DataRequester dataRequester;

    public MessageSender(@NotNull CrawlerConfiguration configuration,
                         @NotNull DataProcessorService dataProcessor,
                         @NotNull DataProviderService dataProviderService,
                         @NotNull IntervalsWorker intervalsWorker,
                         @NotNull StateService<RecoveryState> stateService,
                         @NotNull Handshaker handshaker
    ) {
        this.configuration = configuration;
        this.dataProcessor = dataProcessor;
        this.dataProviderService = dataProviderService;
        this.intervalsWorker = intervalsWorker;
        this.stateService = stateService;
        this.handshaker = handshaker;
        batchSize = configuration.getBatchSize();
        crawlerId = CrawlerId.newBuilder().setName(configuration.getName()).build();
        dataRequester = new DataRequester(dataProviderService);
    }

    public SendingReport sendMessages(MessagesInfo info) throws IOException {
        LOGGER.debug("Sending messages...");

        Interval interval = info.getInterval();
        boolean search = true;
        Timestamp fromTimestamp = MessageUtils.toTimestamp(info.getFrom());
        Timestamp toTimestamp = MessageUtils.toTimestamp(info.getTo());
        Map<StreamKey, MessageID> resumeIds = info.getStartIds();
        Map<StreamKey, InnerMessageId> startIDs = toInnerMessageIDs(resumeIds == null
                ? initialStartIds(fromTimestamp, info.getAliases()) : resumeIds);
        LOGGER.debug("Start IDs for interval: {}", startIDs);
        long numberOfMessages = 0L;

        long diff = 0L;

        String dataProcessorName = info.getDataProcessorInfo().getName();
        String dataProcessorVersion = info.getDataProcessorInfo().getVersion();

        while (search) {

            MessageDataRequest.Builder messageDataBuilder = MessageDataRequest.newBuilder();

            MessagesSearchParameters searchParams = MessagesSearchParameters.builder()
                    .setFrom(fromTimestamp).setTo(toTimestamp).setBatchSize(configuration.getBatchSize())
                    .setResumeIds(resumeIds).setAliases(info.getAliases()).build();
            SearchResult<MessageData> result = dataRequester.searchMessages(searchParams);
            List<MessageData> messages = result.getData();
            LOGGER.debug("Requested {} messages", messages.size());

            if (messages.isEmpty()) {
                LOGGER.info("No more messages in interval from: {}, to: {}", interval.getStartTime(), interval.getEndTime());
                break;
            }

            messages.forEach(m -> {
                ShortId id = ShortId.from(m);
                LOGGER.debug("Sending to the processor message {} with messageType {}", id, m.getMessageType());
            });

            MessageDataRequest messageRequest = messageDataBuilder.setId(crawlerId).addAllMessageData(messages).build();

            MessageResponse response = dataProcessor.sendMessage(messageRequest);

            if (response.hasStatus()) {
                LOGGER.debug("Response has status");
                if (response.getStatus().getHandshakeRequired()) {
                    LOGGER.debug("Handshake required");
                    return handshaker.handshake(crawlerId, interval, info.getDataProcessorInfo(), 0, numberOfMessages);
                }
            }

            List<MessageID> responseIds = response.getIdsList();
            Map.Entry<Integer, Map<StreamKey, InnerMessageId>> processedResult = processServiceResponse(responseIds, result);

            int processedMessagesCount = processedResult == null ? messages.size() : processedResult.getKey();
            numberOfMessages += processedMessagesCount + diff;
            diff = messages.size() - processedMessagesCount;

            if (response.getIdsCount() > 0) {
                requireNonNull(processedResult, () -> "processServiceResponse cannot be null for not empty IDs in response: " + responseIds);
                Map<StreamKey, InnerMessageId> checkpoints = processedResult.getValue();

                if (!checkpoints.isEmpty()) {

                    RecoveryState newState;
                    RecoveryState oldState = stateService.deserialize(interval.getRecoveryState());

                    if (oldState == null) {
                        Map<StreamKey, InnerMessageId> startIntervalIDs = new HashMap<>(startIDs);
                        putAndCheck(checkpoints, startIntervalIDs);

                        newState = new RecoveryState(null, startIntervalIDs, 0, numberOfMessages);
                    } else {
                        Map<StreamKey, InnerMessageId> old = oldState.getLastProcessedMessages();
                        Map<StreamKey, InnerMessageId> lastProcessedMessages = old == null ? new HashMap<>(startIDs) : new HashMap<>(old);
                        putAndCheck(checkpoints, lastProcessedMessages);

                        newState = new RecoveryState(oldState.getLastProcessedEvent(), lastProcessedMessages,
                                oldState.getLastNumberOfEvents(),
                                numberOfMessages);
                    }

                    String recoveryStateJson = stateService.serialize(newState);
                    LOGGER.debug("Recovery state is updated: {}", recoveryStateJson);
                    interval = intervalsWorker.updateRecoveryState(interval, recoveryStateJson);
                }
            }

            resumeIds = requireNonNull(result.getStreamsInfo(), "response from data provider does not have the StreamsInfo")
                    .getStreamsList()
                    .stream()
                    .collect(toUnmodifiableMap(
                            stream -> new StreamKey(stream.getSession(), stream.getDirection()),
                            Stream::getLastId
                    ));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("New resume ids: {}", resumeIds.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + MessageUtils.toJson(entry.getValue()))
                        .collect(Collectors.joining(",")));
            }

            search = messages.size() == batchSize;
        }

        return new SendingReport(CrawlerAction.NONE, interval, dataProcessorName, dataProcessorVersion, 0, numberOfMessages);
    }

    private Map.Entry<@NotNull Integer, @NotNull Map<StreamKey, InnerMessageId>> processServiceResponse(
            List<MessageID> responseIds,
            SearchResult<MessageData> messages
    ) {
        if (responseIds.isEmpty()) {
            return null;
        }
        Map<StreamKey, MessageID> checkpointByDirection = associateWithStreamKey(responseIds.stream(), LATEST_SEQUENCE);

        int messageCount = 0;
        var skipAliases = new HashSet<String>(responseIds.size());
        for (MessageData data : messages.getData()) {
            MessageID messageId = data.getMessageId();
            MessageID checkpointId = checkpointByDirection.get(createStreamKeyFrom(messageId));
            String sessionAlias = messageId.getConnectionId().getSessionAlias();
            if (skipAliases.contains(sessionAlias)) {
                continue;
            }
            messageCount++;
            if (checkpointId.equals(messageId)) {
                skipAliases.add(sessionAlias);
            }
        }

        return Map.entry(messageCount, toInnerMessageIDs(checkpointByDirection));
    }

    private Map<StreamKey, MessageID> initialStartIds(Timestamp fromTimestamp, Collection<String> aliases) {
        int batchSize = 1;
        var parameters = MessagesSearchParameters.builder()
                .setFrom(fromTimestamp)
                .setTo(fromTimestamp)
                .setBatchSize(batchSize)
                .setAliases(aliases)
                .build();
        SearchResult<MessageData> searchResult = dataRequester.searchMessages(parameters);
        SearchResult<MessageData> oppositeRequest = null;

        if (!searchResult.getData().isEmpty()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Initialising start IDs request has returned unexpected data: " + searchResult.getData().stream()
                        .map(MessageUtils::toJson).collect(Collectors.joining(", ")));
                // We have a message with timestamp equal the `fromTimestamp`
                // Because of that we need to make a request in opposite direction
                // and select the first message for each pair alias + direction that were in the response (should be a single message)
                oppositeRequest = dataRequester.searchMessages(MessagesSearchParameters.builder()
                        .setFrom(fromTimestamp)
                        .setBatchSize(batchSize)
                        .setResumeIds(associateWithStreamKey(searchResult.getData().stream().map(MessageData::getMessageId), EARLIEST_SEQUENCE))
                        .setAliases(aliases)
                        .setTimeRelation(TimeRelation.PREVIOUS)
                        .build());
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

    private void putAndCheck(Map<StreamKey, InnerMessageId> checkpoints, Map<StreamKey, InnerMessageId> destination) {
        for (Map.Entry<StreamKey, InnerMessageId> entry : checkpoints.entrySet()) {
            var streamKey = entry.getKey();
            var innerMessageId = entry.getValue();
            InnerMessageId prevInnerMessageId = destination.put(streamKey, innerMessageId);
            if (prevInnerMessageId != null && prevInnerMessageId.getSequence() > innerMessageId.getSequence()) {
                LOGGER.warn("The new checkpoint ID {} has less sequence than the previous one {} for stream key {}",
                        innerMessageId.getSequence(), prevInnerMessageId.getSequence(), streamKey);
            }
        }
    }

    @NotNull
    private Map<StreamKey, InnerMessageId> toInnerMessageIDs(Map<StreamKey, MessageID> checkpointByDirection) {
        return checkpointByDirection.entrySet().stream()
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        it -> fromMessageInfo(it.getValue())
                ));
    }

    private static InnerMessageId fromMessageInfo(MessageID messageID) {
        return new InnerMessageId(
                null,
                messageID.getSequence()
        );
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
    private StreamKey createStreamKeyFrom(MessageID messageID) {
        return new StreamKey(messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
    }
}
