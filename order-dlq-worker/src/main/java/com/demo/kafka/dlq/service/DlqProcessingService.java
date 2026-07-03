package com.demo.kafka.dlq.service;

import com.demo.kafka.common.avro.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DlqProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DlqProcessingService.class);

    public void reprocess(OrderMessage order) {
        log.info("Reprocessando pedido {} da DLQ (cliente={}, valor={})",
                order.getOrderId(), order.getCustomer(), order.getAmount());
        if (order.getFailDlq()) {
            throw new IllegalStateException("Falha simulada no reprocessamento DLQ: " + order.getOrderId());
        }
        log.info("Pedido {} reprocessado com sucesso — removido da fila de retry", order.getOrderId());
    }
}
