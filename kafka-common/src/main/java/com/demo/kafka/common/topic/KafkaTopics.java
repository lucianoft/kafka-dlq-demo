package com.demo.kafka.common.topic;

public final class KafkaTopics {

    public static final String ORDERS = "orders.events";
    public static final String ORDERS_DLQ = "orders.events.dlq";

    public static final String HEADER_RETRY_COUNT = "x-retry-count";
    public static final String HEADER_ORIGINAL_TOPIC = "x-original-topic";
    public static final String HEADER_ERROR_MESSAGE = "x-error-message";
    public static final String HEADER_FAILED_AT = "x-failed-at";

    private KafkaTopics() {
    }
}
