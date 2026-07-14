package com.demo.kafka.api.service;

import com.demo.kafka.common.avro.OrderMessage;
import com.demo.kafka.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * A key do registro Kafka e o customer (nao o orderId): assim todos os pedidos
     * de um mesmo cliente caem na mesma particao e sao consumidos em ordem.
     */
    public CompletableFuture<SendResult<String, OrderMessage>> publish(OrderMessage message) {
        String key = message.getCustomer();
        String orderId = message.getOrderId();
        log.info("Publicando pedido {} (customer={}) no topico {}", orderId, key, KafkaTopics.ORDERS);
        return kafkaTemplate.send(KafkaTopics.ORDERS, key, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Falha ao publicar pedido {} (customer={})", orderId, key, ex);
                    } else {
                        log.info("Pedido {} (customer={}) publicado partition={} offset={}",
                                orderId, key, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }
}
