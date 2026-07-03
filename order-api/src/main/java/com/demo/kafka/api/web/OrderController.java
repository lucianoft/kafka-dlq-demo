package com.demo.kafka.api.web;

import com.demo.kafka.common.model.OrderMessage;
import com.demo.kafka.api.service.OrderProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> publish(@RequestBody PublishOrderRequest request) throws Exception {
        String orderId = request.orderId() != null ? request.orderId() : UUID.randomUUID().toString();
        OrderMessage message = OrderMessage.of(
                orderId,
                request.customer(),
                request.amount(),
                request.failProcessing(),
                request.failDlq()
        );
        producerService.publish(message).get();
        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "status", "published",
                "topic", "orders.events"
        ));
    }

    public record PublishOrderRequest(
            String orderId,
            String customer,
            double amount,
            boolean failProcessing,
            boolean failDlq
    ) {
    }
}
