/*
 *  Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.crawler.AbstractStrategy.AbstractCrawlerData.SizableDataPart;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.metrics.CrawlerMetrics;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.Message;

public abstract class AbstractStrategy<C extends Continuation, P extends DataPart> implements DataTypeStrategy<C, P> {
    protected final CrawlerMetrics metrics;

    public AbstractStrategy(CrawlerMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "'Metrics' parameter");
    }

    public abstract static class AbstractCrawlerData<S, C extends Continuation, DATA extends SizableDataPart<VALUE>, VALUE extends Message>
            extends AbstractIterator<DATA>
            implements CrawlerData<C, DATA> {

        private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCrawlerData.class);

        private final Iterator<S> data;
        private final Deque<VALUE> cache = new ArrayDeque<>();
        private final int maxSize;
        private final CrawlerId crawlerId;
        private int elements;
        private int currentValuesSize;
        private int dropped;
        private boolean finished;

        protected AbstractCrawlerData(Iterator<S> data, CrawlerId id, int maxSize) {
            this.data = Objects.requireNonNull(data, "'Data' parameter");
            if (maxSize <= 0) {
                throw new IllegalArgumentException("not positive maxSize " + maxSize);
            }
            this.maxSize = maxSize;
            crawlerId = Objects.requireNonNull(id, "'Id' parameter");
        }

        @Override
        protected final DATA computeNext() {
            while (data.hasNext()) {
                S response = data.next();
                updateState(response);
                VALUE value = extractValue(response);
                if (value != null) {
                    elements++;
                    VALUE filtered = filterValue(value);
                    if (filtered == null) {
                        dropped++;
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Value with ID {} was dropped. Values inside: {}", extractId(value), extractCount(value));
                        }
                        continue;
                    }
                    cache.addLast(filtered);
                    currentValuesSize += filtered.getSerializedSize();
                    if (currentValuesSize >= maxSize) {
                        return createDataPart(cache);
                    }
                }
            }
            if (!cache.isEmpty()) {
                return createDataPart(cache);
            }
            finished = true;
            if (dropped > 0) {
                LOGGER.info("Dropped {} messages from {} by the filter option in custom config", dropped, elements);
            }
            return endOfData();
        }

        @Nullable
        protected VALUE filterValue(VALUE value) {
            return value;
        }

        @NotNull
        private DATA createDataPart(Deque<VALUE> cache) {
            DATA dataPart = buildDataPart(crawlerId, Collections.unmodifiableCollection(cache));
            cache.clear();
            int pushedBackSize = 0;
            while (dataPart.serializedSize() > maxSize) {
                VALUE last = Objects.requireNonNull(dataPart.pullLast(), "at least one value must be in the data part");
                if (dataPart.getSize() == 0) {
                    throw new IllegalStateException(
                            "Data part cannot be constructed because max size in " + maxSize + " is exceeded and no data fits that size."
                            + " Last value: " + extractId(last)
                    );
                }
                cache.addFirst(last); // put back to the cache to send next try
                pushedBackSize += last.getSerializedSize();
            }
            currentValuesSize = pushedBackSize;
            return dataPart;
        }

        protected abstract String extractId(VALUE last);

        protected abstract void updateState(S response);

        @Nullable
        protected abstract VALUE extractValue(S response);

        protected abstract int extractCount(VALUE value);

        protected abstract DATA buildDataPart(CrawlerId crawlerId, Collection<VALUE> values);

        protected abstract C getContinuationInternal();

        @Nullable
        @Override
        public final C getContinuation() {
            assertFinished();
            return getContinuationInternal();
        }

        @Override
        public final boolean getHasData() {
            assertFinished();
            return elements > 0;
        }

        @Override
        public final int size() {
            assertFinished();
            return elements;
        }

        private void assertFinished() {
            if (!finished) {
                throw new IllegalStateException("it is not allowed to check data before it was fully processed");
            }
        }

        public interface SizableDataPart<T extends Message> extends DataPart {
            int serializedSize();

            @Nullable T pullLast();
        }
    }
}
