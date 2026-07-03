package com.demo.kafka.processor.consumer;

import com.demo.kafka.common.model.OrderMessage;
import com.demo.kafka.common.topic.KafkaTopics;
import com.demo.kafka.processor.service.DlqPublisherService;
import com.demo.kafka.processor.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final OrderProcessingService processingService;
    private final DlqPublisherService dlqPublisherService;

    public OrderEventConsumer(OrderProcessingService processingService,
                              DlqPublisherService dlqPublisherService) {
        this.processingService = processingService;
        this.dlqPublisherService = dlqPublisherService;
    }

    @KafkaListener(topics = KafkaTopics.ORDERS, groupId = "${app.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, OrderMessage> record, Acknowledgment ack) {
        log.info("Recebido pedido {} partition={} offset={}", record.key(), record.partition(), record.offset());
        try {
            processingService.process(record.value());
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Erro ao processar pedido {} — enviando para DLQ", record.key(), ex);
            dlqPublisherService.publishToDlq(record, ex);
            ack.acknowledge();
        }
    }
}
