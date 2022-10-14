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

package com.exactpro.th2.crawler.grpc

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.crawler.CrawlerConfiguration
import com.exactpro.th2.crawler.CrawlerContext
import com.exactpro.th2.dataprovider.grpc.AsyncDataProviderService
import com.exactpro.th2.dataprovider.grpc.BulkEventRequest
import com.exactpro.th2.dataprovider.grpc.BulkEventResponse
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsRequest
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsResponse
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.dataprovider.grpc.EventFiltersRequest
import com.exactpro.th2.dataprovider.grpc.EventMatchRequest
import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.grpc.EventSearchResponse
import com.exactpro.th2.dataprovider.grpc.FilterInfoRequest
import com.exactpro.th2.dataprovider.grpc.FilterInfoResponse
import com.exactpro.th2.dataprovider.grpc.FilterNamesResponse
import com.exactpro.th2.dataprovider.grpc.MatchResponse
import com.exactpro.th2.dataprovider.grpc.MessageFiltersRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageMatchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.TextFormat
import io.grpc.stub.ClientCallStreamObserver
import io.grpc.stub.ClientResponseObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicInteger

//FIXME: this class has been made for test concept
class BlockingService(
    private val context: CrawlerContext,
    private val asyncDataProviderService: AsyncDataProviderService,
) : DataProviderService {

    override fun getEvent(input: EventID): EventResponse {
        return response(input, asyncDataProviderService::getEvent)
    }

    override fun getEvents(input: BulkEventRequest): BulkEventResponse {
        return response(input, asyncDataProviderService::getEvents)
    }

    override fun getMessage(input: MessageID): MessageGroupResponse {
        return response(input, asyncDataProviderService::getMessage)
    }

    override fun getMessageStreams(input: MessageStreamsRequest): MessageStreamsResponse {
        return response(input, asyncDataProviderService::getMessageStreams)
    }

    override fun searchMessages(input: MessageSearchRequest): Iterator<MessageSearchResponse> {
        return responses(input, asyncDataProviderService::searchMessages, context.metrics::setBackpressureBufferSize)
    }

    override fun searchEvents(input: EventSearchRequest): Iterator<EventSearchResponse> {
        return responses(input, asyncDataProviderService::searchEvents, context.metrics::setBackpressureBufferSize)
    }

    override fun loadCradleMessageGroups(input: CradleMessageGroupsRequest): CradleMessageGroupsResponse {
        return response(input, asyncDataProviderService::loadCradleMessageGroups)
    }

    override fun searchMessageGroups(input: MessageGroupsSearchRequest): Iterator<MessageGroupsSearchResponse> {
        return responses(input, asyncDataProviderService::searchMessageGroups, context.metrics::setBackpressureBufferSize)
    }

    override fun getMessagesFilters(input: MessageFiltersRequest): FilterNamesResponse {
        return response(input, asyncDataProviderService::getMessagesFilters)
    }

    override fun getEventsFilters(input: EventFiltersRequest): FilterNamesResponse {
        return response(input, asyncDataProviderService::getEventsFilters)
    }

    override fun getEventFilterInfo(input: FilterInfoRequest): FilterInfoResponse {
        return response(input, asyncDataProviderService::getEventFilterInfo)
    }

    override fun getMessageFilterInfo(input: FilterInfoRequest): FilterInfoResponse {
        return response(input, asyncDataProviderService::getMessageFilterInfo)
    }

    override fun matchEvent(input: EventMatchRequest): MatchResponse {
        return response(input, asyncDataProviderService::matchEvent, )
    }

    override fun matchMessage(input: MessageMatchRequest): MatchResponse {
        return response(input, asyncDataProviderService::matchMessage)
    }

    private fun <ReqT : MessageOrBuilder, RespT : MessageOrBuilder> response(request: ReqT, function: (ReqT, StreamObserver<RespT>) -> Unit): RespT {
        return responses(request, function).next()
    }

    private fun <ReqT : MessageOrBuilder, RespT : MessageOrBuilder> responses(request: ReqT, function: (ReqT, StreamObserver<RespT>) -> Unit, setMetric: (Int) -> Unit = {}): Iterator<RespT> {
        ClientObserver<ReqT, RespT>(
            context.configuration,
            setMetric
        ).apply {
            function.invoke(request, this)
            return iterator
        }
    }

    private class ClientObserver<ReqT : MessageOrBuilder, RespT: MessageOrBuilder>(
        val config: CrawlerConfiguration,
        setMetric: (Int) -> Unit,
    ) : ClientResponseObserver<ReqT, RespT> {
        val iterator = BlockingIterator<RespT>(setMetric)

        @Volatile
        private lateinit var requestStream: ClientCallStreamObserver<ReqT>
        private val counter = AtomicInteger()

        override fun beforeStart(requestStream: ClientCallStreamObserver<ReqT>) {
            LOGGER.debug { "beforeStart has been called" }
            this.requestStream = requestStream
            // Set up manual flow control for the response stream. It feels backwards to configure the response
            // stream's flow control using the request stream's observer, but this is the way it is.
            requestStream.disableAutoRequestWithInitial(config.initialGrpcRequest)
        }

        override fun onNext(value: RespT) {
            LOGGER.debug { "onNext has been called ${TextFormat.shortDebugString(value)}" }
            if (config.debug.enablePeriodicalGrpcRequest) {
                if (counter.incrementAndGet() % config.periodicalGrpcRequest == 0) {
                    requestStream.request(config.periodicalGrpcRequest)
                }
            }
            iterator.put(value)
        }

        override fun onError(t: Throwable) {
            LOGGER.error(t) { "onError has been called" }
            iterator.complete(t)
        }

        override fun onCompleted() {
            LOGGER.debug { "onCompleted has been called" }
            iterator.complete()
        }

        companion object {
            private val LOGGER = KotlinLogging.logger { }
        }
    }
}