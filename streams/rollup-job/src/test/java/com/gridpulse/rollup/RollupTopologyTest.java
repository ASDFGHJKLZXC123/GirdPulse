package com.gridpulse.rollup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.gridpulse.events.RegionRollup;
import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for final regional rollups.
 *
 * <p>Every asserted region receives a same-region trailing clock event strictly beyond its latest
 * asserted window end plus the 30-second grace. M04 has no pre-window speed filter, so these controls
 * need no threshold speed. Controls are outside the target windows and their later clock-only windows
 * are intentionally excluded from assertions.
 */
class RollupTopologyTest {

    private static final String REGISTRY_SCOPE = "mock://rollup-topology-test";
    private static final long MIN = 60_000L;
    private static final long SEC = 1_000L;
    private static final long BASE = 1000 * MIN;
    private static final long FLUSH_TS = BASE + 10 * MIN;

    @AfterEach
    void dropRegistry() {
        MockSchemaRegistry.dropScope(REGISTRY_SCOPE);
    }

    @Test
    void countsSixEventsAndTwoDistinctActiveVehicles() {
        try (Harness h = new Harness("six-events")) {
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 30, BASE + 10 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.ACTIVE, 40, BASE + 15 * SEC);
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 50, BASE + 20 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.ACTIVE, 60, BASE + 25 * SEC);
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 70, BASE + 30 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.ACTIVE, 80, BASE + 35 * SEC);
            h.flush("SEA");

            final RegionRollup rollup = only(h.targetRollups(BASE));
            assertEquals(6L, rollup.getEventCount());
            assertEquals(2, rollup.getActiveVehicles());
            assertEquals(55.0, rollup.getAvgSpeedKph(), 1e-9);
        }
    }

    @Test
    void idleEventsCountButDoNotAffectActiveSpeedMean() {
        try (Harness h = new Harness("idle-mean")) {
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 30, BASE + 10 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.ACTIVE, 50, BASE + 20 * SEC);
            h.pipe("v1", "SEA", VehicleStatus.IDLE, 900, BASE + 30 * SEC);
            h.pipe("v3", "SEA", VehicleStatus.IDLE, 700, BASE + 40 * SEC);
            h.flush("SEA");

            final RegionRollup rollup = only(h.targetRollups(BASE));
            assertEquals(4L, rollup.getEventCount());
            assertEquals(2, rollup.getActiveVehicles());
            assertEquals(40.0, rollup.getAvgSpeedKph(), 1e-9);
        }
    }

    @Test
    void eventsStraddlingMinuteBoundaryLandInSeparateWindows() {
        try (Harness h = new Harness("straddle")) {
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 20, BASE + 59 * SEC);
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 80, BASE + 61 * SEC);
            h.flush("SEA");

            final Map<Long, RegionRollup> byWindow = h.rollups().stream()
                    .filter(kv -> kv.key.equals("SEA"))
                    .map(kv -> kv.value)
                    .filter(r -> r.getWindowStart().toEpochMilli() == BASE
                            || r.getWindowStart().toEpochMilli() == BASE + MIN)
                    .collect(Collectors.toMap(r -> r.getWindowStart().toEpochMilli(), r -> r));

            assertEquals(2, byWindow.size());
            assertEquals(1L, byWindow.get(BASE).getEventCount());
            assertEquals(20.0, byWindow.get(BASE).getAvgSpeedKph(), 1e-9);
            assertEquals(1L, byWindow.get(BASE + MIN).getEventCount());
            assertEquals(80.0, byWindow.get(BASE + MIN).getAvgSpeedKph(), 1e-9);
        }
    }

    @Test
    void distinctVehicleSetDoesNotLeakAcrossWindows() {
        try (Harness h = new Harness("distinct-window")) {
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 20, BASE + 10 * SEC);
            h.pipe("v1", "SEA", VehicleStatus.ACTIVE, 30, BASE + MIN + 10 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.ACTIVE, 40, BASE + MIN + 20 * SEC);
            h.flush("SEA");

            final Map<Long, RegionRollup> byWindow = h.rollups().stream()
                    .map(kv -> kv.value)
                    .filter(r -> r.getWindowStart().toEpochMilli() == BASE
                            || r.getWindowStart().toEpochMilli() == BASE + MIN)
                    .collect(Collectors.toMap(r -> r.getWindowStart().toEpochMilli(), r -> r));

            assertEquals(1, byWindow.get(BASE).getActiveVehicles());
            assertEquals(2, byWindow.get(BASE + MIN).getActiveVehicles());
        }
    }

    @Test
    void allIdleWindowHasZeroAverageNotNan() {
        try (Harness h = new Harness("all-idle")) {
            h.pipe("v1", "SEA", VehicleStatus.IDLE, 0, BASE + 10 * SEC);
            h.pipe("v2", "SEA", VehicleStatus.OFFLINE, 0, BASE + 20 * SEC);
            h.flush("SEA");

            final RegionRollup rollup = only(h.targetRollups(BASE));
            assertEquals(2L, rollup.getEventCount());
            assertEquals(0, rollup.getActiveVehicles());
            assertEquals(0.0, rollup.getAvgSpeedKph(), 0.0);
            assertFalse(Double.isNaN(rollup.getAvgSpeedKph()));
        }
    }

    private static RegionRollup only(List<RegionRollup> rollups) {
        assertEquals(1, rollups.size(), "exactly one final record for the asserted region/window");
        final RegionRollup rollup = rollups.get(0);
        assertEquals("SEA", rollup.getRegion());
        assertEquals(BASE, rollup.getWindowStart().toEpochMilli());
        assertEquals(BASE + MIN, rollup.getWindowEnd().toEpochMilli());
        return rollup;
    }

    private static Map<String, Object> serdeConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", REGISTRY_SCOPE);
        config.put("auto.register.schemas", true);
        config.put("specific.avro.reader", true);
        return config;
    }

    private static VehicleEvent event(
            String vehicleId,
            String region,
            VehicleStatus status,
            double speedKph,
            long occurredAtMillis) {
        return VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId(vehicleId)
                .setRegion(region)
                .setLat(0)
                .setLon(0)
                .setSpeedKph(speedKph)
                .setHeadingDeg(0)
                .setStatus(status)
                .setOccurredAt(Instant.ofEpochMilli(occurredAtMillis))
                .build();
    }

    private static final class Harness implements AutoCloseable {
        private final TopologyTestDriver driver;
        private final TestInputTopic<String, VehicleEvent> input;
        private final TestOutputTopic<String, RegionRollup> output;

        Harness(String tag) {
            final Properties props = new Properties();
            props.put("application.id", "rollup-topology-test-" + tag);
            props.put("bootstrap.servers", "dummy:9092");
            props.put("default.key.serde", Serdes.String().getClass().getName());
            props.put("default.timestamp.extractor", OccurredAtTimestampExtractor.class.getName());

            final SpecificAvroSerde<VehicleEvent> eventSerde = new SpecificAvroSerde<>();
            eventSerde.configure(serdeConfig(), false);
            final SpecificAvroSerde<RegionRollup> rollupSerde = new SpecificAvroSerde<>();
            rollupSerde.configure(serdeConfig(), false);

            driver = new TopologyTestDriver(RollupTopology.build(serdeConfig()), props);
            input = driver.createInputTopic(
                    RollupTopology.INPUT_TOPIC,
                    Serdes.String().serializer(),
                    eventSerde.serializer());
            output = driver.createOutputTopic(
                    RollupTopology.OUTPUT_TOPIC,
                    Serdes.String().deserializer(),
                    rollupSerde.deserializer());
        }

        void pipe(
                String vehicleId,
                String region,
                VehicleStatus status,
                double speedKph,
                long occurredAtMillis) {
            input.pipeInput(
                    vehicleId,
                    event(vehicleId, region, status, speedKph, occurredAtMillis),
                    Instant.ofEpochMilli(occurredAtMillis));
        }

        /** Same-region, outside-window control strictly beyond window end plus grace. */
        void flush(String region) {
            pipe("clock-" + region, region, VehicleStatus.IDLE, 0.0, FLUSH_TS);
        }

        List<KeyValue<String, RegionRollup>> rollups() {
            return output.readKeyValuesToList();
        }

        List<RegionRollup> targetRollups(long windowStart) {
            return rollups().stream()
                    .filter(kv -> kv.key.equals("SEA"))
                    .map(kv -> kv.value)
                    .filter(rollup -> rollup.getWindowStart().toEpochMilli() == windowStart)
                    .collect(Collectors.toList());
        }

        @Override
        public void close() {
            driver.close();
        }
    }
}
