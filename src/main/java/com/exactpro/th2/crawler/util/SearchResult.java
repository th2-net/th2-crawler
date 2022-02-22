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

package com.exactpro.th2.crawler.util;

import java.util.List;
import java.util.Objects;

import com.exactpro.th2.dataprovider.grpc.MessageStreamPointers;
import com.google.protobuf.MessageOrBuilder;

public class SearchResult<T extends MessageOrBuilder> {
    private final List<T> data;
    private final MessageStreamPointers pointers;

    public SearchResult(List<T> data, MessageStreamPointers pointers) {
        this.data = Objects.requireNonNull(data, "'Data' parameter");
        this.pointers = pointers;
    }

    public List<T> getData() {
        return data;
    }

    public MessageStreamPointers getPointers() {
        return pointers;
    }
}
