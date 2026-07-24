package com.gridpulse.rollup;

import com.gridpulse.events.RegionRollup;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/** Per-region, one-minute final rollups over the vehicle event stream. */
public final class RollupTopology {

    public static final String INPUT_TOPIC = "fleet.vehicle-events";
    public static final String OUTPUT_TOPIC = "fleet.rollups.region-1m";

    static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    static final Duration WINDOW_GRACE = Duration.ofSeconds(30);
    static final String STORE_NAME = "region-rollup-store";

    private RollupTopology() {
    }

    public static Topology build(Map<String, ?> serdeConfig) {
        final Serde<String> stringSerde = Serdes.String();

        final SpecificAvroSerde<VehicleEvent> eventSerde = new SpecificAvroSerde<>();
        eventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<RegionRollup> rollupSerde = new SpecificAvroSerde<>();
        rollupSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        final TimeWindows windows = TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE);

        final KTable<Windowed<String>, RollupAggregate> windowed = builder
                .stream(INPUT_TOPIC, Consumed.with(stringSerde, eventSerde))
                // The input is keyed by vehicle. Re-keying by region deliberately creates the
                // internal repartition topic that this milestone demonstrates.
                .groupBy(
                        (vehicleId, event) -> event.getRegion(),
                        Grouped.with(stringSerde, new VehicleEventRepartitionSerde()))
                .windowedBy(windows)
                .aggregate(
                        RollupAggregate::empty,
                        (region, event, aggregate) -> aggregate.add(event),
                        Materialized.<String, RollupAggregate, WindowStore<Bytes, byte[]>>as(STORE_NAME)
                                .withKeySerde(stringSerde)
                                .withValueSerde(new RollupAggregateSerde()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        windowed
                .toStream()
                .map((windowedRegion, aggregate) -> {
                    final String region = windowedRegion.key();
                    final long windowStart = windowedRegion.window().start();
                    final long windowEnd = windowedRegion.window().end();
                    final RegionRollup rollup = RegionRollup.newBuilder()
                            .setRegion(region)
                            .setWindowStart(Instant.ofEpochMilli(windowStart))
                            .setWindowEnd(Instant.ofEpochMilli(windowEnd))
                            .setEventCount(aggregate.eventCount())
                            .setActiveVehicles(aggregate.activeVehicleCount())
                            .setAvgSpeedKph(aggregate.averageActiveSpeedKph())
                            .build();
                    return KeyValue.pair(region, rollup);
                })
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, rollupSerde));

        return builder.build();
    }
}
