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
import java.util.Iterator;
import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.MessageOrBuilder;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupItem;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer;
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointers;

public class MessageSearchResponseAdapter implements Iterable<MessageSearchResponse> {
    private final Collection<MessageSearchResponse> response = new ArrayList<>();

    public MessageSearchResponseAdapter(Collection<Message> messages) {
        MessageStreamPointers.Builder streamInfo = MessageStreamPointers.newBuilder();
        Collection<MessageSearchResponse> messageResponse = messages.stream()
                .peek(msg -> streamInfo.addMessageStreamPointer(createStream(msg)))
                .map(MessageSearchResponseAdapter::createStreamResponse)
                .collect(Collectors.toList());

        response.add(createStreamResponse(streamInfo.build()));
        response.addAll(messageResponse);
    }

    @NotNull
    @Override
    public Iterator<MessageSearchResponse> iterator() {
        return response.iterator();
    }

    private static MessageStreamPointer createStream(MessageOrBuilder msg) {
        MessageMetadata metadata = msg.getMetadata();
        MessageID id = metadata.getId();
        Direction direction = id.getDirection();
        return MessageStreamPointer.newBuilder()
                .setLastId(id)
                .setMessageStream(MessageStream.newBuilder().setDirection(direction).setName(id.getConnectionId().getSessionAlias()))
                .build();
    }

    private static MessageSearchResponse createStreamResponse(MessageStreamPointers streamsInfo) {
        return MessageSearchResponse.newBuilder().setMessageStreamPointers(streamsInfo).build();
    }

    private static MessageSearchResponse createStreamResponse(Message msg) {
        return MessageSearchResponse.newBuilder()
                .setMessage(createMessageGroupResponse(msg))
                .build();
    }

    @NotNull
    public static MessageGroupResponse.Builder createMessageGroupResponse(Message msg) {
            return MessageGroupResponse.newBuilder()
                .setMessageId(msg.getMetadata().getId())
                .addMessageItem(MessageGroupItem.newBuilder().setMessage(msg).build());
    }
}
