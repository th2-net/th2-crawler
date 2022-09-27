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
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorGrpc
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.security.SecureRandom
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Disabled("manual test")
class UnaryThroughputTest {

    private val executor = Executors.newFixedThreadPool(4)
    private lateinit var eventLoop: NioEventLoopGroup
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var service: AutoServer

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
        createAutoServer()
    }

    @Test
    fun `client (ASYNC) test`() {
        val sampler = Sampler(" client (ASYNC)", SAMPLING_FREQUENCY)
        val stub = DataProcessorGrpc.newStub(channel)
        val list = ArrayList<ClientObserver<*>>(SEQUENCE_SIZE)
        for (i in 0 until SEQUENCE_SIZE) {
            val clientObserver = ClientObserver<MessageResponse>(sampler, BATCH_SIZE)
            list.add(clientObserver)
            stub.sendMessage(prototype, clientObserver)
        }
        list.forEach(ClientObserver<*>::await)
        sampler.complete()
    }

    @Test
    fun `client (BLOCKING) test`() {
        val sampler = Sampler(" client (BLOCKING)", SAMPLING_FREQUENCY)
        val stub = DataProcessorGrpc.newBlockingStub(channel)
        for (i in 0 until SEQUENCE_SIZE) {
            stub.sendMessage(prototype)
            sampler.inc(BATCH_SIZE)
        }
        sampler.complete()
    }

    @AfterEach
    fun afterEach() {
        service.stop()
        channel.shutdown()
            .awaitTermination(5, TimeUnit.SECONDS)
        channel.shutdownNow()

        server.shutdown()

        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.SECONDS)
        executor.shutdownNow()
    }

    private fun createChannel() {
//        channel = InProcessChannelBuilder.forName(NAME)
//            .directExecutor() // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
//            .usePlaintext().build()

        channel = ManagedChannelBuilder.forAddress(HOST, PORT)
            .usePlaintext()
            .keepAliveTime(60, TimeUnit.SECONDS)
            .maxInboundMessageSize(Int.MAX_VALUE)
            .build()
    }

    private fun createServer() {
        eventLoop = NioEventLoopGroup(2, executor)
        server = NettyServerBuilder
            .forPort(PORT)
            .workerEventLoopGroup(eventLoop)
            .bossEventLoopGroup(eventLoop)
            .channelType(NioServerSocketChannel::class.java)
            .keepAliveTime(60, TimeUnit.SECONDS)
            .maxInboundMessageSize(Int.MAX_VALUE)
            .addService(service)
            .build()
            .start()

        createChannel()
    }

    private fun createAutoServer() {
        service = AutoServer(SAMPLING_FREQUENCY)
        createServer()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val RANDOM = SecureRandom()
        private const val HOST = "localhost"
        private const val PORT = 8090

        private const val STRING_LENGTH = 256
        private const val BATCH_SIZE = 4_000

        private const val SEQUENCE_SIZE = 1_000

        const val SAMPLING_FREQUENCY = 100L * BATCH_SIZE

        init {
            check(SEQUENCE_SIZE > SAMPLING_FREQUENCY / BATCH_SIZE) {
                "The $SAMPLING_FREQUENCY sampling frequency is less than the $SEQUENCE_SIZE sequence size"
            }
        }

        private fun rndString(): String = String(ByteArray(STRING_LENGTH).apply(RANDOM::nextBytes))
    }
}