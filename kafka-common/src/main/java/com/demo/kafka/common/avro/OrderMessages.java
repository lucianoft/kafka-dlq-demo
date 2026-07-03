package com.demo.kafka.common.avro;

import java.time.Instant;

public final class OrderMessages {

    private OrderMessages() {
    }

    public static OrderMessage create(String orderId, String customer, double amount,
                                      boolean failProcessing, boolean failDlq) {
        return OrderMessage.newBuilder()
                .setOrderId(orderId)
                .setCustomer(customer)
                .setAmount(amount)
                .setFailProcessing(failProcessing)
                .setFailDlq(failDlq)
                .setCreatedAt(Instant.now().toEpochMilli())
                .build();
    }
}
