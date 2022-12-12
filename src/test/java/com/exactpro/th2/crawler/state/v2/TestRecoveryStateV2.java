/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.state.v2;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.crawler.state.StateService;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Map;

import static com.exactpro.th2.crawler.CrawlerManager.BOOK_NAME;
import static com.exactpro.th2.crawler.CrawlerManager.SCOPE_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestRecoveryStateV2 {
    private final StateService<RecoveryState> stateService =
            StateService.createFromClasspath(RecoveryState.class, Mockito.mock(DataProviderService.class), null);

    @Test
    void deserializeV1() {
        String json = "{\"version\":\"V_1\",\"lastProcessedEvent\":{\"startTimestamp\":\"2022-12-08T13:33:27.553639Z\",\"id\":\"event_id\"},\"lastProcessedMessages\":[{\"key\":{\"sessionAlias\":\"test\",\"direction\":\"FIRST\"},\"message\":{\"timestamp\":\"2022-12-08T13:33:27.554946Z\",\"sequence\":42}},{\"key\":{\"sessionAlias\":\"test\",\"direction\":\"SECOND\"},\"message\":{\"timestamp\":\"2022-12-08T13:33:27.554955Z\",\"sequence\":43}}],\"lastNumberOfEvents\":10,\"lastNumberOfMessages\":15}";
        assertThrows(IllegalStateException.class, () -> stateService.deserialize(json));
    }

    @Test
    void correctlySerializesAndDeserializes() {
        var state = new RecoveryState(
                new InnerEventId(
                        BOOK_NAME,
                        SCOPE_NAME,
                        Instant.now(),
                        "event_id"
                ),
                Map.of(
                        new StreamKey(BOOK_NAME,"test", Direction.FIRST),
                        new InnerMessageId(Instant.now(), 42L),
                        new StreamKey(BOOK_NAME,"test", Direction.SECOND),
                        new InnerMessageId(Instant.now(), 43L)
                ),
                10,
                15
        );
        String json = assertDoesNotThrow(() -> stateService.serialize(state), () -> "Cannot convert state to json: " + state);
        RecoveryState actualState = assertDoesNotThrow(() -> stateService.deserialize(json), () -> "Cannot deserialize the state from: " + json);
        assertEquals(state, actualState, () -> "Deserialized state: '" + actualState + "' is not the same as the original one: " + state);
    }
}