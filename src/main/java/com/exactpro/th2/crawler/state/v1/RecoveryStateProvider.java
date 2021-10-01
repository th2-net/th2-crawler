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

import com.exactpro.th2.crawler.state.BaseState;
import com.exactpro.th2.crawler.state.StateProvider;
import com.exactpro.th2.crawler.state.StdStateConverter;
import com.exactpro.th2.crawler.state.Version;
import com.google.auto.service.AutoService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@AutoService(StateProvider.class)
public class RecoveryStateProvider implements StateProvider {
    @NotNull
    @Override
    public Version getVersion() {
        return Version.V_1;
    }

    @NotNull
    @Override
    public Class<RecoveryState> getStateClass() {
        return RecoveryState.class;
    }

    @Nullable
    @Override
    public StdStateConverter<RecoveryState, BaseState> getConverter() {
        return null;
    }
}
