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

package com.exactpro.th2.crawler.state;

import java.time.Instant;
import java.util.Map;

import com.exactpro.th2.common.grpc.Direction;
import org.junit.jupiter.api.Test;

import static com.exactpro.th2.crawler.state.RecoveryState.getStateFromJson;
import static org.junit.jupiter.api.Assertions.*;

class TestRecoveryState {
    @Test
    void correctlySerializesAndDeserializes() {
        var state = new RecoveryState(
                new InnerEventId(
                        Instant.now(),
                        "event_id"
                ),
                Map.of(
                        new StreamKey("test", Direction.FIRST),
                        new InnerMessageId("test", Instant.now(), Direction.FIRST, 42L),
                        new StreamKey("test", Direction.SECOND),
                        new InnerMessageId("test", Instant.now(), Direction.SECOND, 43L)
                ),
                10,
                15
        );
        String json = assertDoesNotThrow(state::convertToJson, () -> "Cannot convert state to json: " + state);
        RecoveryState actualState = assertDoesNotThrow(() -> getStateFromJson(json), () -> "Cannot deserialize the state from: " + json);
        assertEquals(state, actualState, () -> "Deserialized state: '" + actualState + "' is not the same as the original one: " + state);
    }
}