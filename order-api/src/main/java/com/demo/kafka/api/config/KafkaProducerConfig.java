package com.demo.kafka.api.config;

import com.demo.kafka.common.model.OrderMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.ssl.trust-store-location}")
    private String trustStoreLocation;

    @Value("${app.kafka.ssl.key-store-location}")
    private String keyStoreLocation;

    @Value("${app.kafka.ssl.store-password}")
    private String storePassword;

    @Bean
    public ProducerFactory<String, OrderMessage> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", trustStoreLocation);
        props.put("ssl.truststore.password", storePassword);
        props.put("ssl.keystore.location", keyStoreLocation);
        props.put("ssl.keystore.password", storePassword);
        props.put("ssl.key.password", storePassword);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OrderMessage> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
