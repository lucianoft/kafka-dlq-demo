package com.demo.kafka.common.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public final class KafkaAvroConfigSupport {

    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private KafkaAvroConfigSupport() {
    }

    public static Map<String, Object> sslProps(String trustStoreLocation, String keyStoreLocation,
                                               String storePassword) {
        Map<String, Object> ssl = new HashMap<>();
        ssl.put("security.protocol", "SSL");
        ssl.put("ssl.truststore.location", trustStoreLocation);
        ssl.put("ssl.truststore.password", storePassword);
        ssl.put("ssl.keystore.location", keyStoreLocation);
        ssl.put("ssl.keystore.password", storePassword);
        ssl.put("ssl.key.password", storePassword);
        return ssl;
    }

    public static void applyProducerAvro(Map<String, Object> props, String schemaRegistryUrl) {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    }

    public static void applyConsumerAvro(Map<String, Object> props, String schemaRegistryUrl) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }
}
