package com.demo.kafka.api.service;

import com.demo.kafka.common.avro.OrderMessage;
import com.demo.kafka.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, OrderMessage>> publish(OrderMessage message) {
        String key = message.getOrderId() != null ? message.getOrderId() : UUID.randomUUID().toString();
        log.info("Publicando pedido {} no topico {}", key, KafkaTopics.ORDERS);
        return kafkaTemplate.send(KafkaTopics.ORDERS, key, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Falha ao publicar pedido {}", key, ex);
                    } else {
                        log.info("Pedido {} publicado offset={}", key, result.getRecordMetadata().offset());
                    }
                });
    }
}
