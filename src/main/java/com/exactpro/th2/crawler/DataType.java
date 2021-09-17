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

import java.util.Objects;
import java.util.stream.Stream;

public enum DataType {
    EVENTS("EVENTS"), MESSAGES("MESSAGES");

    private final String typeName;

    DataType(String type) {
        this.typeName = Objects.requireNonNull(type, "'Type' parameter");
    }

    public String getTypeName() {
        return typeName;
    }

    public static DataType byTypeName(String typeName) {
        Objects.requireNonNull(typeName, "'Type name' parameter");
        return Stream.of(values())
                .filter(it -> it.getTypeName().equals(typeName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type name: " + typeName));
    }
}
