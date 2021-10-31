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

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.dataprovider.grpc.MessageData;

public class ShortId {
    private final String sessionAlias;
    private final String direction;
    private final long sequence;

    public ShortId(String sessionAlias, String direction, long sequence) {
        this.sessionAlias = sessionAlias;
        this.direction = direction;
        this.sequence = sequence;
    }

    @Override
    public String toString() {
        return sessionAlias + ':' + direction + ':' + sequence;
    }

    public static ShortId from(MessageData m) {
        MessageID messageId = m.getMessageId();
        return new ShortId(m.getSessionId().getSessionAlias(), messageId.getDirection().name(), messageId.getSequence());
    }
}
