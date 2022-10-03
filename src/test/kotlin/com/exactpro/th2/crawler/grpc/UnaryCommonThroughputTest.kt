/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.crawler.grpc

import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.factory.FactorySettings
import com.exactpro.th2.crawler.dataprocessor.grpc.AsyncDataProcessorService
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorGrpc
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorService
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import io.grpc.ManagedChannel
import io.grpc.Server
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.nio.file.Paths
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

@Disabled("manual test")
class UnaryCommonThroughputTest {

    private lateinit var common: AbstractCommonFactory

    private lateinit var server: Server
    private lateinit var service: AutoServer
    private lateinit var blockingClient: DataProcessorService
    private lateinit var asyncClient: AsyncDataProcessorService

    private val prototype = MessageDataRequest.newBuilder().apply {
        messageDataBuilderList.apply {
            for (i in 0 until BATCH_SIZE) {
                addMessageData(MessageGroupResponse.newBuilder().apply {
                    addMessageItem(MessageGroupItem.newBuilder().apply {
                        messageBuilder.apply {
                            addField("raw", rndString())
                        }
                    })
                }.build())
            }
        }
    }.build()

    @BeforeEach
    fun beforeEach() {
        LOGGER.info { "batch size is ${prototype.serializedSize} bytes, $BATCH_SIZE messages" }

        common = CommonFactory(FactorySettings().apply {
            grpc = getPath("com/exactpro/th2/crawler/grpc/grpc.json")
            routerGRPC = getPath("com/exactpro/th2/crawler/grpc/grpc_router.json")
        })

        blockingClient = common.grpcRouter.getService(DataProcessorService::class.java)
        asyncClient = common.grpcRouter.getService(AsyncDataProcessorService::class.java)
        service = AutoServer(UnaryThroughputTest.SAMPLING_FREQUENCY)
        server = common.grpcRouter.startServer(service).apply(Server::start)
    }

    @Test
    fun `client (ASYNC) test`() {
        val sampler = Sampler(" client (ASYNC)", SAMPLING_FREQUENCY)
        val list = ArrayList<ClientObserver<*>>(SEQUENCE_SIZE)
        for (i in 0 until SEQUENCE_SIZE) {
            val clientObserver = ClientObserver<MessageResponse>(sampler, BATCH_SIZE)
            list.add(clientObserver)
            asyncClient.sendMessage(prototype, clientObserver)
        }
        list.forEach(ClientObserver<*>::await)
        sampler.complete()
    }

    @Test
    fun `client (BLOCKING) test`() {
        val sampler = Sampler(" client (BLOCKING)", SAMPLING_FREQUENCY)
        for (i in 0 until SEQUENCE_SIZE) {
            blockingClient.sendMessage(prototype)
            sampler.inc(BATCH_SIZE)
        }
        sampler.complete()
    }

    @AfterEach
    fun afterEach() {
        service.stop()
        if(!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow()
            check(server.awaitTermination(5, TimeUnit.SECONDS)) {
                "Server can not terminate"
            }
        }

        common.close()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val RANDOM = SecureRandom()

        private const val STRING_LENGTH = 256
        private const val BATCH_SIZE = 4_000

        private const val SEQUENCE_SIZE = 1_000

        const val SAMPLING_FREQUENCY = 100L * BATCH_SIZE

        init {
            check(SEQUENCE_SIZE > SAMPLING_FREQUENCY / BATCH_SIZE) {
                "The $SAMPLING_FREQUENCY sampling frequency is less than the $SEQUENCE_SIZE sequence size"
            }
        }

        private fun rndString(): String = RANDOM.ints(32, 126)
            .limit(STRING_LENGTH.toLong() - 1)
            .collect({ StringBuilder() }, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString() + "\u0001"

        private fun getPath(resourceName: String) =
            Paths.get(
                requireNotNull(Thread.currentThread().contextClassLoader.getResource(resourceName)) {

                }.toURI()
            )
    }
}