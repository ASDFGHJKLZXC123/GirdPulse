package com.gridpulse.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gridpulse.events.Anomaly;
import com.gridpulse.events.AnomalyKind;
import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TopologyTestDriver (unit, no broker) coverage of the anomaly topology.
 *
 * <p><b>Clock-control rule (CCR-008):</b> every valid vehicle event reaches the hopping window, while
 * only {@code speed_kph > 120.0} mutates the violation aggregate. Same-key normal-speed records
 * therefore advance the same task/partition's stream time and flush closed violation windows without
 * creating or changing an anomaly. A full-hop-set flush lies beyond the latest containing window's
 * {@code window_end + 30s grace}; the focused latency fixture advances only beyond the earliest
 * containing window's close and proves first output in less than two event-time minutes.
 *
 * <p><b>Hopping caveat:</b> a single spike at {@code t} falls into up to 5 overlapping 5-min windows
 * (advance 1 min) ⇒ up to 5 anomalies is correct. Assertions check per-window uniqueness, never a
 * single total.
 */
class AnomalyTopologyTest {

    private static final String REGISTRY_SCOPE = "mock://anomaly-topology-test";
    private static final long MIN = 60_000L;
    private static final long SEC = 1_000L;
    private static final long WINDOW = 5 * MIN;
    private static final long GRACE = 30 * SEC;
    // Epoch-aligned base (multiple of the 1-min window advance) so window boundaries are easy to reason about.
    private static final long BASE = 1000 * MIN;
    // Far-future flush timestamp: shares no 5-min window with any burst around BASE.
    private static final long FLUSH_TS = BASE + 60 * MIN;

    private static final String VEHICLE = "v1";

    @AfterEach
    void dropRegistry() {
        MockSchemaRegistry.dropScope(REGISTRY_SCOPE);
    }

    // (a) all speeds <= 120 (+ trailing clock event) -> no output
    @Test
    void noAnomalyWhenAllSpeedsBelowThreshold() {
        try (Harness h = new Harness("a")) {
            h.pipe(VEHICLE, 90.0, BASE + 10 * SEC);
            h.pipe(VEHICLE, 120.0, BASE + 20 * SEC); // exactly 120 does not mutate the aggregate
            // Normal traffic advances stream time but must not create an anomaly.
            h.pipe(VEHICLE, 0.0, FLUSH_TS);

            assertTrue(h.anomaliesFor(VEHICLE).isEmpty(), "no anomaly expected when nothing exceeds threshold");
        }
    }

    // (b) one spike + flush -> one anomaly per closed containing window (up to 5), correct value + bounds
    @Test
    void oneSpikeEmitsOneAnomalyPerContainingWindow() {
        try (Harness h = new Harness("b")) {
            long spikeTs = BASE + 2 * MIN;
            h.pipe(VEHICLE, 150.0, spikeTs);
            h.flush(VEHICLE, FLUSH_TS); // final > 120 event, same key, far future

            List<Anomaly> anomalies = h.anomaliesFor(VEHICLE);

            // A spike at BASE+2min lands in exactly 5 hopping windows: starts BASE-2min .. BASE+2min.
            assertEquals(5, anomalies.size(), "one anomaly per closed containing window (hop set of 5)");

            Set<Long> windowStarts = new HashSet<>();
            for (Anomaly a : anomalies) {
                long start = a.getWindowStart().toEpochMilli();
                long end = a.getWindowEnd().toEpochMilli();
                assertTrue(windowStarts.add(start), "per-window uniqueness: no duplicate window_start");
                assertEquals(start + WINDOW, end, "window_end = window_start + 5min");
                assertTrue(start <= spikeTs && spikeTs < end, "spike falls inside its window");
                assertEquals(150.0, a.getValue(), 1e-9, "value = maxSpeed");
                assertEquals(VEHICLE, a.getVehicleId());
                assertEquals("west", a.getRegion());
                assertEquals(AnomalyKind.SPEED_THRESHOLD, a.getKind());
                assertEquals(1, a.getDetectorVersion());
                assertEquals(AnomalyIds.thresholdAnomalyId(VEHICLE, start), a.getAnomalyId(),
                        "anomaly_id is deterministic UUIDv5 of {vehicle}|{window_start}|SPEED_THRESHOLD");
            }
            Set<Long> expectedStarts = Set.of(
                    BASE - 2 * MIN, BASE - 1 * MIN, BASE, BASE + 1 * MIN, BASE + 2 * MIN);
            assertEquals(expectedStarts, windowStarts, "exact hop set of containing windows");
        }
    }

    // (c) late event within 30s grace -> counted
    @Test
    void lateEventWithinGraceIsCounted() {
        try (Harness h = new Harness("c")) {
            // Window under test W0 = [BASE, BASE+5min), closes at BASE+5min+30s.
            // BASE+320s => aggregate closeTime BASE+290s < W0 end BASE+300s => W0 open.
            h.pipe(VEHICLE, 130.0, BASE + 250 * SEC);          // first violation in W0
            h.pipe(VEHICLE, 60.0, BASE + 320 * SEC);           // normal traffic advances time; W0 still open
            h.pipe(VEHICLE, 150.0, BASE + 260 * SEC);          // late (out-of-order) but within grace -> counted
            h.flush(VEHICLE, FLUSH_TS);

            Anomaly w0 = h.anomalyForWindow(VEHICLE, BASE);
            assertEquals(150.0, w0.getValue(), 1e-9, "late-but-in-grace violation is folded into the window");
        }
    }

    // (d) event later than grace -> dropped
    @Test
    void lateEventAfterGraceIsDropped() {
        try (Harness h = new Harness("d")) {
            // Normal event at BASE+340s => aggregate closeTime BASE+310s >= W0 end
            // BASE+300s => W0 is expired when the late e2 arrives.
            h.pipe(VEHICLE, 130.0, BASE + 250 * SEC);          // first violation in W0
            h.pipe(VEHICLE, 60.0, BASE + 340 * SEC);           // advance the clock PAST W0 close (5min+30s)
            h.pipe(VEHICLE, 150.0, BASE + 260 * SEC);          // arrives after grace for W0 -> dropped from W0
            h.flush(VEHICLE, FLUSH_TS);

            Anomaly w0 = h.anomalyForWindow(VEHICLE, BASE);
            assertEquals(130.0, w0.getValue(), 1e-9, "post-grace violation is dropped from the closed window");
        }
    }

    // (e) two identical runs -> identical anomaly_ids
    @Test
    void anomalyIdsAreDeterministicAcrossRuns() {
        List<String> first = runIdsForSpikeBurst("e1");
        List<String> second = runIdsForSpikeBurst("e2");
        assertEquals(first, second, "identical input reprocessed yields identical anomaly_ids");
        assertTrue(first.size() >= 1);
    }

    // (f) one spike + ordinary traffic -> first anomaly within two event-time minutes
    @Test
    void ordinaryTrafficClosesFirstSpikeWindowWithinTwoMinutesWithoutAnotherViolation() {
        try (Harness h = new Harness("f")) {
            long spikeTs = BASE + 10 * SEC;
            long normalClockTs = BASE + 91 * SEC;
            long earliestWindowStart = BASE - 4 * MIN;

            h.pipe(VEHICLE, "west", 150.0, spikeTs);
            assertTrue(h.anomaliesFor(VEHICLE).isEmpty(), "suppression holds the open spike window");

            // The only later event is ordinary traffic. Its different region also proves that a
            // non-violation cannot overwrite the violation aggregate's lastRegion.
            h.pipe(VEHICLE, "east", 60.0, normalClockTs);

            List<Anomaly> anomalies = h.anomaliesFor(VEHICLE);
            assertEquals(1, anomalies.size(), "the earliest containing window closes on normal traffic");
            Anomaly first = anomalies.get(0);
            assertEquals(earliestWindowStart, first.getWindowStart().toEpochMilli());
            assertEquals(150.0, first.getValue(), 1e-9, "normal traffic cannot change maxSpeed");
            assertEquals("west", first.getRegion(), "normal traffic cannot change lastRegion");
            assertEquals(
                    AnomalyIds.thresholdAnomalyId(VEHICLE, earliestWindowStart),
                    first.getAnomalyId(),
                    "deterministic id is unchanged");
            assertTrue(normalClockTs - spikeTs < 2 * MIN, "first emission clock is within two minutes");
        }
    }

    private List<String> runIdsForSpikeBurst(String tag) {
        try (Harness h = new Harness(tag)) {
            h.pipe(VEHICLE, 150.0, BASE + 2 * MIN);
            h.pipe(VEHICLE, 160.0, BASE + 2 * MIN + SEC);
            h.flush(VEHICLE, FLUSH_TS);
            return h.anomaliesFor(VEHICLE).stream()
                    .map(a -> a.getAnomalyId().toString())
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    // ---- test harness ----

    private static Map<String, Object> serdeConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", REGISTRY_SCOPE);
        config.put("auto.register.schemas", true); // mock registry, unit-test only
        config.put("specific.avro.reader", true);
        return config;
    }

    private static VehicleEvent event(
            String vehicleId, String region, double speedKph, long occurredAtMillis) {
        return VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId(vehicleId).setRegion(region).setLat(0).setLon(0)
                .setSpeedKph(speedKph).setHeadingDeg(0)
                .setStatus(VehicleStatus.ACTIVE)
                .setOccurredAt(Instant.ofEpochMilli(occurredAtMillis))
                .build();
    }

    private static final class Harness implements AutoCloseable {
        private final TopologyTestDriver driver;
        private final TestInputTopic<String, VehicleEvent> input;
        private final TestOutputTopic<String, Anomaly> output;

        Harness(String tag) {
            Properties props = new Properties();
            props.put("application.id", "anomaly-topology-test-" + tag);
            props.put("bootstrap.servers", "dummy:9092");
            props.put("default.key.serde", Serdes.String().getClass().getName());
            props.put("default.timestamp.extractor", OccurredAtTimestampExtractor.class.getName());

            SpecificAvroSerde<VehicleEvent> eventSerde = new SpecificAvroSerde<>();
            eventSerde.configure(serdeConfig(), false);
            SpecificAvroSerde<Anomaly> anomalySerde = new SpecificAvroSerde<>();
            anomalySerde.configure(serdeConfig(), false);

            this.driver = new TopologyTestDriver(AnomalyTopology.build(serdeConfig()), props);
            this.input = driver.createInputTopic(
                    AnomalyTopology.INPUT_TOPIC, Serdes.String().serializer(), eventSerde.serializer());
            this.output = driver.createOutputTopic(
                    AnomalyTopology.OUTPUT_TOPIC, Serdes.String().deserializer(), anomalySerde.deserializer());
        }

        void pipe(String vehicleId, double speedKph, long occurredAtMillis) {
            pipe(vehicleId, "west", speedKph, occurredAtMillis);
        }

        void pipe(String vehicleId, String region, double speedKph, long occurredAtMillis) {
            input.pipeInput(
                    vehicleId,
                    event(vehicleId, region, speedKph, occurredAtMillis),
                    Instant.ofEpochMilli(occurredAtMillis));
        }

        /** Final flush: normal traffic on the same key, far future — evicts closed violation windows. */
        void flush(String vehicleId, long occurredAtMillis) {
            pipe(vehicleId, 60.0, occurredAtMillis);
        }

        List<Anomaly> anomaliesFor(String vehicleId) {
            return output.readKeyValuesToList().stream()
                    .filter(kv -> kv.key.equals(vehicleId))
                    .map(kv -> kv.value)
                    .collect(Collectors.toList());
        }

        Anomaly anomalyForWindow(String vehicleId, long windowStartMillis) {
            List<Anomaly> matches = output.readKeyValuesToList().stream()
                    .filter(kv -> kv.key.equals(vehicleId))
                    .map(kv -> kv.value)
                    .filter(a -> a.getWindowStart().toEpochMilli() == windowStartMillis)
                    .collect(Collectors.toList());
            assertEquals(1, matches.size(), "exactly one anomaly for window_start=" + windowStartMillis);
            return matches.get(0);
        }

        @Override
        public void close() {
            driver.close();
        }
    }
}
