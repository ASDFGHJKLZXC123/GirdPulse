package com.gridpulse.rollup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gridpulse.events.RegionRollup;
import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/** Real Redpanda + Schema Registry coverage of the region repartition and final rollups. */
@Testcontainers
class RollupIntegrationTest {

    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v26.1.12"))
                    // Integration state is intentionally disposable. Keeping it in tmpfs avoids
                    // Docker Desktop low-disk false failures and leaves no anonymous data volume.
                    // The pinned broker otherwise rejects all writes below 5 GiB free, which is
                    // larger than Docker Desktop's default 3.875 GiB container tmpfs. Override only
                    // this disposable broker's threshold; production keeps the pinned defaults.
                    .withTmpFs(Map.of("/var/lib/redpanda/data", "rw"));

    private static final long MIN = 60_000L;
    private static final long SEC = 1_000L;
    private static final long BASE = 1000 * MIN;
    private static final long FLUSH_TS = BASE + 3 * MIN;

    @BeforeAll
    static void configureDisposableBrokerForTmpfs() throws Exception {
        final var update = REDPANDA.execInContainer(
                "rpk", "cluster", "config", "set", "storage_min_free_bytes", "67108864");
        assertEquals(0, update.getExitCode(), update.getStderr());

        final var read = REDPANDA.execInContainer(
                "rpk", "cluster", "config", "get", "storage_min_free_bytes");
        assertEquals(0, read.getExitCode(), read.getStderr());
        assertEquals("67108864", read.getStdout().trim());
    }

    @Test
    @Timeout(180)
    void scriptedMinuteAcrossTwoRegionsProducesExpectedDecodedRollups() throws Exception {
        final String bootstrap = REDPANDA.getBootstrapServers();
        final String registry = REDPANDA.getSchemaRegistryAddress();

        createTopics(bootstrap);
        registerSchemas(registry);

        final KafkaStreams streams = new KafkaStreams(
                RollupTopology.build(serdeConfig(registry)), streamsConfig(bootstrap));
        try {
            streams.start();
            awaitRunning(streams);
            produceInput(bootstrap, registry);

            final Map<String, RegionRollup> target = consumeTargetRollups(bootstrap, registry);
            assertEquals(
                    2,
                    target.size(),
                    "one decoded target-window rollup per scripted region: " + target.keySet());

            assertRollup(target.get("SEA"), "SEA", 3L, 2, 50.0);
            assertRollup(target.get("SFO"), "SFO", 3L, 1, 40.0);
        } finally {
            streams.close(Duration.ofSeconds(20));
        }
    }

    private static void createTopics(String bootstrap) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(RollupTopology.INPUT_TOPIC, 6, (short) 1),
                    new NewTopic(RollupTopology.OUTPUT_TOPIC, 3, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw e;
            }
        }
    }

    private static void registerSchemas(String registry) throws Exception {
        final Path schemasDir = Path.of(System.getProperty("gridpulse.schemas.dir"));
        final SchemaRegistryClient client = new CachedSchemaRegistryClient(registry, 10);
        client.register(
                RollupTopology.INPUT_TOPIC + "-value",
                new AvroSchema(Files.readString(schemasDir.resolve("vehicle-event.v1.avsc"))));
        client.register(
                RollupTopology.OUTPUT_TOPIC + "-value",
                new AvroSchema(Files.readString(schemasDir.resolve("region-rollup.v1.avsc"))));
    }

    private static void produceInput(String bootstrap, String registry) throws Exception {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG, true);

        try (KafkaProducer<String, VehicleEvent> producer = new KafkaProducer<>(props)) {
            final List<ProducerRecord<String, VehicleEvent>> records = List.of(
                    record("sea-1", "SEA", VehicleStatus.ACTIVE, 40, BASE + 10 * SEC),
                    record("sea-2", "SEA", VehicleStatus.ACTIVE, 60, BASE + 20 * SEC),
                    record("sea-1", "SEA", VehicleStatus.IDLE, 0, BASE + 30 * SEC),
                    record("sfo-1", "SFO", VehicleStatus.ACTIVE, 30, BASE + 15 * SEC),
                    record("sfo-1", "SFO", VehicleStatus.ACTIVE, 50, BASE + 25 * SEC),
                    record("sfo-2", "SFO", VehicleStatus.IDLE, 0, BASE + 35 * SEC),
                    // M04 has no threshold filter. Each asserted region/task receives its own
                    // control strictly beyond BASE+1min+30s; later control-only windows are ignored.
                    record("clock-sea", "SEA", VehicleStatus.IDLE, 0, FLUSH_TS),
                    record("clock-sfo", "SFO", VehicleStatus.IDLE, 0, FLUSH_TS));
            for (ProducerRecord<String, VehicleEvent> record : records) {
                // Observe delivery errors directly instead of letting flush retry until test timeout.
                producer.send(record).get();
            }
            producer.flush();
        }
    }

    private static ProducerRecord<String, VehicleEvent> record(
            String vehicleId,
            String region,
            VehicleStatus status,
            double speed,
            long timestamp) {
        final VehicleEvent event = VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId(vehicleId)
                .setRegion(region)
                .setLat(0)
                .setLon(0)
                .setSpeedKph(speed)
                .setHeadingDeg(0)
                .setStatus(status)
                .setOccurredAt(Instant.ofEpochMilli(timestamp))
                .build();
        // Keep this scripted sequence ordered at the source while retaining canonical vehicle
        // keys. The topology still has to repartition the records by region before aggregation.
        return new ProducerRecord<>(RollupTopology.INPUT_TOPIC, 0, vehicleId, event);
    }

    private static Map<String, RegionRollup> consumeTargetRollups(String bootstrap, String registry) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rollup-it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        final Map<String, RegionRollup> target = new HashMap<>();
        try (KafkaConsumer<String, RegionRollup> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(RollupTopology.OUTPUT_TOPIC));
            final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (target.size() < 2 && System.nanoTime() < deadline) {
                final ConsumerRecords<String, RegionRollup> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RegionRollup> record : records) {
                    if (record.value().getWindowStart().toEpochMilli() == BASE) {
                        target.put(record.key(), record.value());
                    }
                }
            }
        }
        return target;
    }

    private static void assertRollup(
            RegionRollup rollup,
            String region,
            long eventCount,
            int activeVehicles,
            double averageSpeed) {
        assertEquals(region, rollup.getRegion());
        assertEquals(BASE, rollup.getWindowStart().toEpochMilli());
        assertEquals(BASE + MIN, rollup.getWindowEnd().toEpochMilli());
        assertEquals(eventCount, rollup.getEventCount());
        assertEquals(activeVehicles, rollup.getActiveVehicles());
        assertEquals(averageSpeed, rollup.getAvgSpeedKph(), 1e-9);
    }

    private static Map<String, Object> serdeConfig(String registry) {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry,
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true,
                KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG, true);
    }

    private static Properties streamsConfig(String bootstrap) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rollup-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                OccurredAtTimestampExtractor.class.getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("rollup-it-state").toString());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void awaitRunning(KafkaStreams streams) throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (streams.state() != State.RUNNING && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(State.RUNNING, streams.state(), "Streams app reached RUNNING before producing");
    }
}
