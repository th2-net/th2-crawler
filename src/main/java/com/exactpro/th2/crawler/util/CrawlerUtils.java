package com.exactpro.th2.crawler.util;

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest;
import com.exactpro.th2.dataprovider.grpc.MessageData;
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest;
import com.exactpro.th2.dataprovider.grpc.StreamResponse;
import com.exactpro.th2.dataprovider.grpc.StringList;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

// TODO: use functional interface
public class CrawlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerUtils.class);


    public static <T> Iterator<StreamResponse> searchEvents(Function<EventSearchRequest, Iterator<StreamResponse>> function,
                                                            EventsSearchInfo<T> info) {

        EventSearchRequest.Builder eventSearchBuilder = (EventSearchRequest.Builder) info.searchBuilder;
        EventSearchRequest request;

        eventSearchBuilder
                .setMetadataOnly(BoolValue.newBuilder().setValue(false).build())
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to)
                .setResultCountLimit(Int32Value.of(info.batchSize));

        if (info.resumeId == null)
            request = eventSearchBuilder.build();
        else
            request = eventSearchBuilder.setResumeFromId(info.resumeId).build();

        return function.apply(request);
    }

    public static <T> Iterator<StreamResponse> searchMessages(Function<MessageSearchRequest, Iterator<StreamResponse>> function,
                                                            MessagesSearchInfo<T> info) {

        MessageSearchRequest.Builder messageSearchBuilder = (MessageSearchRequest.Builder) info.searchBuilder;
        MessageSearchRequest request;

        messageSearchBuilder
                .setStartTimestamp(info.from)
                .setEndTimestamp(info.to)
                .setResultCountLimit(Int32Value.of(info.batchSize))
                .setStream(StringList.newBuilder().addAllListString(info.aliases).build());

        if (info.resumeIds == null)
            request = messageSearchBuilder.build();
        else
            request = messageSearchBuilder.addAllMessageId(info.resumeIds.values()).build();

        return function.apply(request);
    }

    public static List<EventData> collectEvents(Iterator<StreamResponse> iterator, Timestamp to) {
        List<EventData> data = new ArrayList<>();

        while (iterator.hasNext()) {
            StreamResponse r = iterator.next();

            if (r.hasEvent()) {
                EventData event = r.getEvent();

                if (!event.getStartTimestamp().equals(to)) {
                    data.add(event);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Got event {}", MessageUtils.toJson(event, true));
                    }
                }
            }
        }

        return data;
    }

    public static List<MessageData> collectMessages(Iterator<StreamResponse> iterator, Timestamp to) {
        List<MessageData> messages = new ArrayList<>();

        while (iterator.hasNext()) {
            StreamResponse r = iterator.next();

            if (r.hasMessage()) {
                MessageData message = r.getMessage();

                if (!message.getTimestamp().equals(to)) {
                    messages.add(message);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Got message {}", MessageUtils.toJson(message, true));
                    }
                }
            }
        }

        return messages;
    }

    public static class EventsSearchInfo<BuilderT> {
        private final BuilderT searchBuilder;
        private final Timestamp from;
        private final Timestamp to;
        private final int batchSize;
        private final EventID resumeId;

        public EventsSearchInfo(BuilderT searchBuilder, Timestamp from, Timestamp to, int batchSize, EventID resumeId) {
            this.searchBuilder = searchBuilder;
            this.from = from;
            this.to = to;
            this.batchSize = batchSize;
            this.resumeId = resumeId;
        }
    }

    public static class MessagesSearchInfo<BuilderT> {
        private final BuilderT searchBuilder;
        private final Timestamp from;
        private final Timestamp to;
        private final int batchSize;
        private final Map<String, MessageID> resumeIds;
        private final Collection<String> aliases;

        public MessagesSearchInfo(BuilderT searchBuilder, Timestamp from, Timestamp to, int batchSize,
                                  Map<String, MessageID> resumeIds, Collection<String> aliases) {
            this.searchBuilder = searchBuilder;
            this.from = from;
            this.to = to;
            this.batchSize = batchSize;
            this.resumeIds = resumeIds;
            this.aliases = aliases;
        }
    }
}
