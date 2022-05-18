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

package com.exactpro.th2.crawler.state.v1;

import java.time.Instant;
import java.util.Map;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.dataprovider.grpc.DataProviderService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.exactpro.th2.crawler.TestUtilKt.createRecoveryState;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestRecoveryState {
    private final StateService<RecoveryState> stateService =
            StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);
    @Test
    void correctlySerializesAndDeserializes() {
        var state = createRecoveryState();
        String json = assertDoesNotThrow(() -> stateService.serialize(state), () -> "Cannot convert state to json: " + state);
        RecoveryState actualState = assertDoesNotThrow(() -> stateService.deserialize(json), () -> "Cannot deserialize the state from: " + json);
        assertEquals(state, actualState, () -> "Deserialized state: '" + actualState + "' is not the same as the original one: " + state);
    }
}