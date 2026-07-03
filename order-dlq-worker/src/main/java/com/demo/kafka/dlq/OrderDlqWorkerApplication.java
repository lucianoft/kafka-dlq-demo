package com.demo.kafka.dlq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderDlqWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderDlqWorkerApplication.class, args);
    }
}
