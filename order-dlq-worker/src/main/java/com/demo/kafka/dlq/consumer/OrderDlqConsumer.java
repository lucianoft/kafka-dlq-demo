package com.demo.kafka.dlq.consumer;

import com.demo.kafka.common.model.OrderMessage;
import com.demo.kafka.common.support.KafkaMessageHeaders;
import com.demo.kafka.common.topic.KafkaTopics;
import com.demo.kafka.dlq.service.DlqProcessingService;
import com.demo.kafka.dlq.service.DlqRepublishService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderDlqConsumer.class);

    private final DlqProcessingService dlqProcessingService;
    private final DlqRepublishService dlqRepublishService;

    public OrderDlqConsumer(DlqProcessingService dlqProcessingService,
                            DlqRepublishService dlqRepublishService) {
        this.dlqProcessingService = dlqProcessingService;
        this.dlqRepublishService = dlqRepublishService;
    }

    @KafkaListener(topics = KafkaTopics.ORDERS_DLQ, groupId = "${app.kafka.consumer.group-id}")
    public void consumeDlq(ConsumerRecord<String, OrderMessage> record, Acknowledgment ack) {
        int retry = KafkaMessageHeaders.getRetryCount(record);
        log.info("DLQ: pedido {} offset={} retry={}", record.key(), record.offset(), retry);

        try {
            dlqProcessingService.reprocess(record.value());
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("DLQ: falha ao reprocessar pedido {} — ack + reenvio para mesma DLQ", record.key(), ex);
            ack.acknowledge();
            dlqRepublishService.republishToSameDlq(record, ex);
        }
    }
}
