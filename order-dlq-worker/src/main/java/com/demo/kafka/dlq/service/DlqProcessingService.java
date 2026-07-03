package com.demo.kafka.dlq.service;

import com.demo.kafka.common.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DlqProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DlqProcessingService.class);

    public void reprocess(OrderMessage order) {
        log.info("Reprocessando pedido {} da DLQ (cliente={}, valor={})",
                order.orderId(), order.customer(), order.amount());
        if (order.failDlq()) {
            throw new IllegalStateException("Falha simulada no reprocessamento DLQ: " + order.orderId());
        }
        log.info("Pedido {} reprocessado com sucesso — removido da fila de retry", order.orderId());
    }
}
