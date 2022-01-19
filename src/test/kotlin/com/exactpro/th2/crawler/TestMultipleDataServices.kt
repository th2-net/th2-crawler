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

package com.exactpro.th2.crawler

import com.exactpro.th2.crawler.dataprocessor.grpc.AsyncDataProcessorService
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageDataRequest
import com.exactpro.th2.crawler.dataprocessor.grpc.MessageResponse
import io.grpc.stub.StreamObserver
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.stubbing.OngoingStubbing

class TestMultipleDataServices {
    @Test
    fun `multiple processors`() {
        val processor1: AsyncDataProcessorService = mock {
            on { sendMessage(any(), any()) }.thenObserverAnswer<MessageDataRequest, MessageResponse> {
                listOf()
            }
        }
    }

    private fun <REQ, RESP> OngoingStubbing<Unit>.thenObserverAnswer(answers: (REQ) -> List<RESP>): OngoingStubbing<Unit> = apply {
        doAnswer {
            val req = it.getArgument<REQ>(0)
            val observer = it.getArgument<StreamObserver<RESP>>(0)
            answers(req).forEach(observer::onNext)
            null
        }
    }
}