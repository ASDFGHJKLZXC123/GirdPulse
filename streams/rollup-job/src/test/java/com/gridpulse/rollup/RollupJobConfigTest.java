package com.gridpulse.rollup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class RollupJobConfigTest {

    @Test
    void usesMilestoneFixedStreamsConfiguration() {
        final Properties config = RollupJob.streamsConfig();

        assertEquals(RollupJob.APPLICATION_ID, config.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(RollupJob.brokers(), config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StreamsConfig.EXACTLY_ONCE_V2, config.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        assertEquals(
                OccurredAtTimestampExtractor.class.getName(),
                config.get(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
    }

    @Test
    void usesExistingSpecificAvroSchemasWithoutAutoRegistration() {
        final Map<String, Object> config = RollupJob.serdeConfig();

        assertEquals(
                RollupJob.schemaRegistryUrl(),
                config.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        assertEquals(false, config.get(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS));
        assertEquals(true, config.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG));
        assertEquals(true, config.get(KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG));
    }
}
