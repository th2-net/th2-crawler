/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.crawler.messages.strategy;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.crawler.CrawlerConfiguration;
import com.exactpro.th2.crawler.CrawlerData;
import com.exactpro.th2.crawler.DataParameters;
import com.exactpro.th2.crawler.filters.NameFilter;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.MessagePart;
import com.exactpro.th2.crawler.messages.strategy.MessagesCrawlerData.ResumeMessageIDs;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.exactpro.th2.crawler.state.v2.StreamKey;
import com.exactpro.th2.crawler.util.CrawlerUtils;
import com.exactpro.th2.crawler.util.MessagesSearchParameters;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MessagesGroupStrategy extends AbstractMessagesStrategy {
    private final String book;
    private final Set<String> groups;
    public MessagesGroupStrategy(
            @NotNull DataProviderService provider,
            @NotNull CrawlerMetrics metrics,
            @NotNull CrawlerConfiguration config
    ) {
        super(provider, metrics, config);
        if(config.getBookToGroups().size() != 1) {
            throw new IllegalStateException("Current version supports only one book instead of " + config.getBookToGroups().entrySet());
        }
        Optional<Map.Entry<String, Set<String>>> entry = config.getBookToGroups().entrySet().stream().findFirst();
        if (entry.isPresent()) {
            this.book = entry.get().getKey();
            this.groups = entry.get().getValue();
        } else {
            throw new IllegalStateException("First book is not presented");
        }
    }

    @NotNull
    @Override
    public CrawlerData<ResumeMessageIDs, MessagePart> requestData(@NotNull Timestamp start, @NotNull Timestamp end, @NotNull DataParameters parameters,
                                                                  @Nullable ResumeMessageIDs continuation) {
        requireNonNull(start, "'start' parameter");
        requireNonNull(end, "'end' parameter");
        requireNonNull(parameters, "'parameters' parameter");
        Map<StreamKey, MessageID> resumeIds = continuation == null ? null : continuation.getIds();
        Map<StreamKey, MessageID> startIDs = resumeIds == null ? Collections.emptyMap() : resumeIds; //TODO: remove start ids
        MessagesSearchParameters searchParams = MessagesSearchParameters.builder()
                .setFrom(start)
                .setTo(end)
                .setResumeIds(resumeIds)
                .setBook(book)
                .setStreamIds(groups)
                .build();

        NameFilter filter = config.getFilter();
        return new MessagesCrawlerData(
                CrawlerUtils.searchByGroups(provider, searchParams, metrics),
                startIDs,
                parameters.getCrawlerId(),
                config.getMaxOutgoingDataSize(),
                msg -> filter == null || filter.accept(msg.getMetadata().getMessageType())
        );
    }
}
