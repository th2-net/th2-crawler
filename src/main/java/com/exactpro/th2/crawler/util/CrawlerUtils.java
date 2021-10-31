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

package com.exactpro.th2.crawler.util;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.crawler.state.v1.InnerEventId;
import com.exactpro.th2.crawler.state.v1.InnerMessageId;
import com.exactpro.th2.crawler.state.v1.RecoveryState;
import com.exactpro.th2.crawler.state.v1.StreamKey;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);

    public static final BinaryOperator<MessageID> LATEST_SEQUENCE = (first, second) -> first.getSequence() < second.getSequence() ? second : first;
    public static final BinaryOperator<MessageID> EARLIEST_SEQUENCE = (first, second) -> first.getSequence() > second.getSequence() ? second : first;


    public static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Interval updateEventRecoveryState(
            IntervalsWorker worker,
            Interval interval,
            StateService<RecoveryState> stateService,
            long numberOfEvents
    ) throws IOException {
        RecoveryState currentState = stateService.deserialize(interval.getRecoveryState());
        InnerEventId lastProcessedEvent = null;

        if (currentState != null) {
            lastProcessedEvent = currentState.getLastProcessedEvent();
        }


        RecoveryState newState;
        if (currentState == null) {
            newState = new RecoveryState(
                    lastProcessedEvent,
                    null,
                    numberOfEvents,
                    0);
        } else {
            newState = new RecoveryState(
                    lastProcessedEvent,
                    currentState.getLastProcessedMessages(),
                    numberOfEvents,
                    currentState.getLastNumberOfMessages());
        }

        return worker.updateRecoveryState(interval, stateService.serialize(newState));
    }

    public static Interval updateMessageRecoveryState(
            IntervalsWorker worker,
            Interval interval,
            StateService<RecoveryState> stateService,
            long numberOfMessages
    ) throws IOException {
        RecoveryState currentState = interval.getRecoveryState() == null
                ? null
                : stateService.deserialize(interval.getRecoveryState());
        Map<StreamKey, InnerMessageId> lastProcessedMessages = new HashMap<>();

        if (currentState != null && currentState.getLastProcessedMessages() != null) {
            lastProcessedMessages.putAll(currentState.getLastProcessedMessages());
        }

        RecoveryState newState;
        if (currentState == null) {
            newState = new RecoveryState(
                    null,
                    lastProcessedMessages,
                    0,
                    numberOfMessages
            );
        } else {
            newState = new RecoveryState(
                    currentState.getLastProcessedEvent(),
                    lastProcessedMessages,
                    currentState.getLastNumberOfEvents(),
                    numberOfMessages);
        }

        return worker.updateRecoveryState(interval, stateService.serialize(newState));
    }
}
