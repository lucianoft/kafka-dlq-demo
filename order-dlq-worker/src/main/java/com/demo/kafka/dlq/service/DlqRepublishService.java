package com.demo.kafka.dlq.service;

import com.demo.kafka.common.avro.OrderMessage;
import com.demo.kafka.common.support.KafkaMessageHeaders;
import com.demo.kafka.common.topic.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DlqRepublishService {

    private static final Logger log = LoggerFactory.getLogger(DlqRepublishService.class);

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public DlqRepublishService(KafkaTemplate<String, OrderMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Ack ja foi feito pelo consumer — reenvia para a mesma DLQ com retry incrementado.
     */
    public void republishToSameDlq(ConsumerRecord<String, OrderMessage> record, Exception error) {
        ProducerRecord<String, OrderMessage> dlqRecord = new ProducerRecord<>(
                KafkaTopics.ORDERS_DLQ,
                null,
                record.key(),
                record.value()
        );

        for (var header : KafkaMessageHeaders.copyWithRetry(record, error.getMessage())) {
            dlqRecord.headers().add(header);
        }

        kafkaTemplate.send(dlqRecord);
        int retry = KafkaMessageHeaders.getRetryCount(record) + 1;
        log.warn("Pedido {} reenviado para DLQ (retry={}). Motivo: {}", record.key(), retry, error.getMessage());
    }
}
