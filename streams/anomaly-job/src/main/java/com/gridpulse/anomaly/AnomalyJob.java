package com.gridpulse.anomaly;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Entry point: assembles the anomaly-detection Kafka Streams application with exactly-once (v2)
 * semantics and starts it. Broker and Schema Registry endpoints are environment-driven with
 * localhost defaults.
 */
public final class AnomalyJob {

    public static final String APPLICATION_ID = "gridpulse-anomaly-job";
    private static final String DEFAULT_BROKERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private AnomalyJob() {
    }

    static String brokers() {
        return env("BROKERS", DEFAULT_BROKERS);
    }

    static String schemaRegistryUrl() {
        return env("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL);
    }

    /** Kafka Streams application config with the milestone-fixed guarantees. */
    static Properties streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                OccurredAtTimestampExtractor.class.getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    /**
     * Config for the Confluent Avro serdes: point at the registry, never auto-register (schemas are
     * owned by {@code make schemas}), and decode into generated {@code SpecificRecord} classes.
     */
    static Map<String, Object> serdeConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return config;
    }

    public static void main(String[] args) {
        final Topology topology = AnomalyTopology.build(serdeConfig());
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig());

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            streams.close();
        }
    }

    private static String env(String name, String fallback) {
        final String value = System.getenv(name);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
