package com.gridpulse.anomaly;

import com.gridpulse.events.Anomaly;
import com.gridpulse.events.AnomalyKind;
import com.gridpulse.events.VehicleEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Kafka Streams topology for v1 speed-threshold anomaly detection.
 *
 * <pre>
 *   stream(fleet.vehicle-events)
 *     -> filter(speed_kph > 120.0)
 *     -> groupByKey -> windowedBy(hopping 5min/1min, grace 30s)
 *     -> aggregate({violationCount, maxSpeed, lastRegion})
 *     -> suppress(untilWindowCloses(unbounded))
 *     -> map to Anomaly (deterministic UUIDv5 id)
 *     -> to(fleet.anomalies) keyed by vehicle_id
 * </pre>
 *
 * Every serde is passed explicitly (never inferred from stream defaults) so the reader-schema /
 * registry config is unambiguous.
 */
public final class AnomalyTopology {

    public static final String INPUT_TOPIC = "fleet.vehicle-events";
    public static final String OUTPUT_TOPIC = "fleet.anomalies";

    public static final double SPEED_THRESHOLD = 120.0;
    public static final int DETECTOR_VERSION = 1;

    static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    static final Duration WINDOW_ADVANCE = Duration.ofMinutes(1);
    static final Duration WINDOW_GRACE = Duration.ofSeconds(30);

    static final String STORE_NAME = "anomaly-violation-store";

    private AnomalyTopology() {
    }

    /**
     * Builds the topology. {@code serdeConfig} configures the Confluent Avro serdes — the same map
     * for app runtime ({@code schema.registry.url=http://...}) and unit tests ({@code mock://...}).
     */
    public static Topology build(Map<String, ?> serdeConfig) {
        final Serde<String> keySerde = Serdes.String();

        final SpecificAvroSerde<VehicleEvent> eventSerde = new SpecificAvroSerde<>();
        eventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Anomaly> anomalySerde = new SpecificAvroSerde<>();
        anomalySerde.configure(serdeConfig, false);

        final Serde<ViolationAggregate> aggregateSerde = new ViolationAggregateSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, VehicleEvent> events =
                builder.stream(INPUT_TOPIC, Consumed.with(keySerde, eventSerde));

        final TimeWindows windows = TimeWindows
                .ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE)
                .advanceBy(WINDOW_ADVANCE);

        final KTable<Windowed<String>, ViolationAggregate> windowed = events
                .filter((vehicleId, event) -> event.getSpeedKph() > SPEED_THRESHOLD)
                .groupByKey(Grouped.with(keySerde, eventSerde))
                .windowedBy(windows)
                .aggregate(
                        ViolationAggregate::empty,
                        (vehicleId, event, aggregate) ->
                                aggregate.add(event.getSpeedKph(), event.getRegion()),
                        Materialized.<String, ViolationAggregate, WindowStore<Bytes, byte[]>>as(STORE_NAME)
                                .withKeySerde(keySerde)
                                .withValueSerde(aggregateSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        windowed
                .toStream()
                .filter((windowedKey, aggregate) -> aggregate != null && aggregate.violationCount() >= 1)
                .map((windowedKey, aggregate) -> {
                    final String vehicleId = windowedKey.key();
                    final long windowStart = windowedKey.window().start();
                    final long windowEnd = windowedKey.window().end();
                    final Anomaly anomaly = Anomaly.newBuilder()
                            .setAnomalyId(AnomalyIds.thresholdAnomalyId(vehicleId, windowStart))
                            .setVehicleId(vehicleId)
                            .setRegion(aggregate.lastRegion())
                            .setKind(AnomalyKind.SPEED_THRESHOLD)
                            .setValue(aggregate.maxSpeed())
                            .setDetectorVersion(DETECTOR_VERSION)
                            .setWindowStart(Instant.ofEpochMilli(windowStart))
                            .setWindowEnd(Instant.ofEpochMilli(windowEnd))
                            .build();
                    return KeyValue.pair(vehicleId, anomaly);
                })
                .to(OUTPUT_TOPIC, Produced.with(keySerde, anomalySerde));

        return builder.build();
    }
}
