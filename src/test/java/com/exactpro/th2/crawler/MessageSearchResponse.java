/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageData.Builder;
import com.exactpro.th2.dataprovider.grpc.Stream;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StreamsInfo;

import org.jetbrains.annotations.NotNull;

public class MessageSearchResponse implements Iterable<StreamResponse> {
    private final List<StreamResponse> response = new ArrayList<>();

    public MessageSearchResponse(Collection<Message> messages) {
        Collection<StreamResponse> messageResponse = new ArrayList<>();

        StreamsInfo.Builder streamInfo = StreamsInfo.newBuilder();
        for (Message msg : messages) {
            streamInfo.addStreams(createStream(msg));
            messageResponse.add(createStreamResponse(msg));
        }

        response.add(createStreamResponse(streamInfo.build()));
        response.addAll(messageResponse);
    }

    public static @NotNull Builder createMessageData(Message msg) {
        MessageID id = msg.getMetadata().getId();
        return MessageData.newBuilder()
                .setMessage(msg)
                .setMessageId(id)
                .setDirection(id.getDirection())
                .setSessionId(id.getConnectionId())
                .setTimestamp(msg.getMetadata().getTimestamp());
    }

    @NotNull
    @Override
    public Iterator<StreamResponse> iterator() {
        return response.iterator();
    }

    public Iterator<StreamResponse> streamInfoIterator() {
        return Collections.singleton(response.get(0)).iterator();
    }

    private static Stream createStream(Message msg) {
        MessageMetadata metadata = msg.getMetadata();
        MessageID id = metadata.getId();
        Direction direction = id.getDirection();
        return Stream.newBuilder().setDirection(direction).setSession(id.getConnectionId().getSessionAlias()).build();
    }

    private static StreamResponse createStreamResponse(StreamsInfo streamsInfo) {
        return StreamResponse.newBuilder().setStreamInfo(streamsInfo).build();
    }

    private static StreamResponse createStreamResponse(Message msg) {
        return StreamResponse.newBuilder().setMessage(createMessageData(msg)).build();
    }
}
