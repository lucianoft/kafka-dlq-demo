package com.demo.kafka.processor.service;

import com.demo.kafka.common.avro.OrderMessage;
import com.demo.kafka.common.support.KafkaMessageHeaders;
import com.demo.kafka.common.topic.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class DlqPublisherService {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisherService.class);

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public DlqPublisherService(KafkaTemplate<String, OrderMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishToDlq(ConsumerRecord<String, OrderMessage> record, Exception error) {
        ProducerRecord<String, OrderMessage> dlqRecord = new ProducerRecord<>(
                KafkaTopics.ORDERS_DLQ,
                null,
                record.key(),
                record.value()
        );

        dlqRecord.headers().add(new RecordHeader(
                KafkaTopics.HEADER_ORIGINAL_TOPIC,
                KafkaTopics.ORDERS.getBytes(StandardCharsets.UTF_8)));

        for (var header : KafkaMessageHeaders.copyWithRetry(record, error.getMessage())) {
            dlqRecord.headers().add(header);
        }

        kafkaTemplate.send(dlqRecord);
        log.warn("Pedido {} enviado para DLQ. Motivo: {}", record.key(), error.getMessage());
    }
}
