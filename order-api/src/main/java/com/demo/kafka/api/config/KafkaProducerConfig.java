package com.demo.kafka.api.config;

import com.demo.kafka.common.avro.OrderMessage;
import com.demo.kafka.common.config.KafkaAvroConfigSupport;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${app.kafka.ssl.trust-store-location}")
    private String trustStoreLocation;

    @Value("${app.kafka.ssl.key-store-location}")
    private String keyStoreLocation;

    @Value("${app.kafka.ssl.store-password}")
    private String storePassword;

    @Bean
    public ProducerFactory<String, OrderMessage> producerFactory() {
        Map<String, Object> props = new HashMap<>(
                KafkaAvroConfigSupport.sslProps(trustStoreLocation, keyStoreLocation, storePassword));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        KafkaAvroConfigSupport.applyProducerAvro(props, schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OrderMessage> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
