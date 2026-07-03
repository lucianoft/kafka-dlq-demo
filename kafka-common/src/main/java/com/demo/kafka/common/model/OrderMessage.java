package com.demo.kafka.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderMessage(
        String orderId,
        String customer,
        double amount,
        /** Simula falha no consumer principal → vai para DLQ */
        boolean failProcessing,
        /** Simula falha no worker DLQ → ack + reenvio para mesma DLQ */
        boolean failDlq,
        Instant createdAt
) {
    public static OrderMessage of(String orderId, String customer, double amount,
                                  boolean failProcessing, boolean failDlq) {
        return new OrderMessage(orderId, customer, amount, failProcessing, failDlq, Instant.now());
    }
}
