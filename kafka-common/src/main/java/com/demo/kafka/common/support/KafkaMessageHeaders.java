package com.demo.kafka.common.support;

import com.demo.kafka.common.topic.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class KafkaMessageHeaders {

    private KafkaMessageHeaders() {
    }

    public static int getRetryCount(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(KafkaTopics.HEADER_RETRY_COUNT);
        if (header == null || header.value() == null) {
            return 0;
        }
        return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
    }

    public static List<Header> copyWithRetry(ConsumerRecord<?, ?> record, String errorMessage) {
        List<Header> headers = new ArrayList<>();
        record.headers().forEach(headers::add);

        int retry = getRetryCount(record) + 1;
        headers.removeIf(h -> KafkaTopics.HEADER_RETRY_COUNT.equals(h.key()));
        headers.removeIf(h -> KafkaTopics.HEADER_ERROR_MESSAGE.equals(h.key()));
        headers.removeIf(h -> KafkaTopics.HEADER_FAILED_AT.equals(h.key()));

        headers.add(new RecordHeader(KafkaTopics.HEADER_RETRY_COUNT,
                String.valueOf(retry).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(KafkaTopics.HEADER_ERROR_MESSAGE,
                errorMessage.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(KafkaTopics.HEADER_FAILED_AT,
                Instant.now().toString().getBytes(StandardCharsets.UTF_8)));
        return headers;
    }
}
