package com.demo.kafka.processor.service;

import com.demo.kafka.common.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    public void process(OrderMessage order) {
        log.info("Processando pedido {} cliente={} valor={}", order.orderId(), order.customer(), order.amount());
        if (order.failProcessing()) {
            throw new IllegalStateException("Falha simulada no processamento principal: " + order.orderId());
        }
        log.info("Pedido {} processado com sucesso", order.orderId());
    }
}
