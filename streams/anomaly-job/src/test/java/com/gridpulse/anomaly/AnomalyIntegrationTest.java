package com.gridpulse.anomaly;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gridpulse.events.Anomaly;
import com.gridpulse.events.AnomalyKind;
import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test against a real Redpanda broker + Schema Registry (Testcontainers). Registers both
 * v1 subjects, runs the actual EOS-v2 topology, produces a spike burst plus a trailing flush event
 * past window-end + grace, and asserts decoded {@link Anomaly} records land on {@code fleet.anomalies}
 * with no duplicate {@code (vehicle_id, window_start)} pairs.
 */
@Testcontainers
class AnomalyIntegrationTest {

    // Pinned to the same image the project runs in docker-compose.
    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v26.1.12"));

    private static final long MIN = 60_000L;
    private static final long SEC = 1_000L;
    private static final long BASE = 1000 * MIN;
    private static final long FLUSH_TS = BASE + 60 * MIN;
    private static final String VEHICLE = "v-int";

    @Test
    @Timeout(180)
    void spikeBurstProducesDeduplicatedAnomalies() throws Exception {
        final String bootstrap = REDPANDA.getBootstrapServers();
        final String registry = REDPANDA.getSchemaRegistryAddress();

        createTopics(bootstrap);
        registerSchemas(registry);
        produceInput(bootstrap, registry);

        final KafkaStreams streams = new KafkaStreams(
                AnomalyTopology.build(serdeConfig(registry)), streamsConfig(bootstrap, registry));
        try {
            streams.start();
            final List<Anomaly> anomalies = consumeAnomalies(bootstrap, registry);

            assertFalse(anomalies.isEmpty(), "expected at least one anomaly on fleet.anomalies");

            final Set<String> seen = new HashSet<>();
            for (Anomaly a : anomalies) {
                assertTrue(a.getValue() > 120.0, "anomaly value must exceed the threshold");
                assertTrue(a.getKind() == AnomalyKind.SPEED_THRESHOLD, "kind is SPEED_THRESHOLD");
                final String pair = a.getVehicleId() + "|" + a.getWindowStart().toEpochMilli();
                assertTrue(seen.add(pair), "no duplicate (vehicle_id, window_start): " + pair);
            }
        } finally {
            streams.close(Duration.ofSeconds(20));
        }
    }

    private static void createTopics(String bootstrap) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(AnomalyTopology.INPUT_TOPIC, 6, (short) 1),
                    new NewTopic(AnomalyTopology.OUTPUT_TOPIC, 6, (short) 1))).all().get();
        } catch (ExecutionException e) {
            // Ignore "already exists" if the container is reused across retries.
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw e;
            }
        }
    }

    private static void registerSchemas(String registry) throws Exception {
        final SchemaRegistryClient client = new CachedSchemaRegistryClient(registry, 10);
        client.register(AnomalyTopology.INPUT_TOPIC + "-value", new AvroSchema(VehicleEvent.getClassSchema()));
        client.register(AnomalyTopology.OUTPUT_TOPIC + "-value", new AvroSchema(Anomaly.getClassSchema()));
    }

    private static void produceInput(String bootstrap, String registry) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        try (KafkaProducer<String, VehicleEvent> producer = new KafkaProducer<>(props)) {
            // Three spikes in one window burst, same key -> same partition/task.
            producer.send(record(VEHICLE, 150.0, BASE + 1 * SEC));
            producer.send(record(VEHICLE, 155.0, BASE + 2 * SEC));
            producer.send(record(VEHICLE, 160.0, BASE + 3 * SEC));
            // Trailing flush event past window-end + grace: > 120 and same key so it reaches the
            // suppress node on the burst's partition and evicts the burst's closed windows. Its own
            // far-future window never closes, so it emits nothing.
            producer.send(record(VEHICLE, 200.0, FLUSH_TS));
            producer.flush();
        }
    }

    private static ProducerRecord<String, VehicleEvent> record(String vehicleId, double speed, long ts) {
        return new ProducerRecord<>(AnomalyTopology.INPUT_TOPIC, vehicleId, event(vehicleId, speed, ts));
    }

    private static VehicleEvent event(String vehicleId, double speedKph, long occurredAtMillis) {
        return VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId(vehicleId).setRegion("west").setLat(0).setLon(0)
                .setSpeedKph(speedKph).setHeadingDeg(0)
                .setStatus(VehicleStatus.ACTIVE)
                .setOccurredAt(Instant.ofEpochMilli(occurredAtMillis))
                .build();
    }

    private static List<Anomaly> consumeAnomalies(String bootstrap, String registry) throws Exception {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "anomaly-it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        // EOS producers write transaction markers; read only committed records.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        final java.util.ArrayList<Anomaly> anomalies = new java.util.ArrayList<>();
        try (KafkaConsumer<String, Anomaly> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(AnomalyTopology.OUTPUT_TOPIC));
            final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            long lastReceive = 0L;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, Anomaly> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Anomaly> r : records) {
                    anomalies.add(r.value());
                    lastReceive = System.nanoTime();
                }
                // Once results start arriving, drain a short quiet window then stop.
                if (!anomalies.isEmpty() && lastReceive != 0L
                        && System.nanoTime() - lastReceive > Duration.ofSeconds(5).toNanos()) {
                    break;
                }
            }
        }
        return anomalies;
    }

    private static Map<String, Object> serdeConfig(String registry) {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry,
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    private static Properties streamsConfig(String bootstrap, String registry) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                OccurredAtTimestampExtractor.class.getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("anomaly-it-state").toString());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}
