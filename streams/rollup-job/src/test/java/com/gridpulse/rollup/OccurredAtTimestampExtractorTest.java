package com.gridpulse.rollup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class OccurredAtTimestampExtractorTest {

    private final OccurredAtTimestampExtractor extractor = new OccurredAtTimestampExtractor();

    @Test
    void returnsOccurredAtRatherThanBrokerTimestamp() {
        final long occurredAtMillis = 1_700_000_123_456L;
        final VehicleEvent event = VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId("v1")
                .setRegion("SEA")
                .setLat(0)
                .setLon(0)
                .setSpeedKph(42)
                .setHeadingDeg(0)
                .setStatus(VehicleStatus.ACTIVE)
                .setOccurredAt(Instant.ofEpochMilli(occurredAtMillis))
                .build();
        final ConsumerRecord<Object, Object> record =
                new ConsumerRecord<>(RollupTopology.INPUT_TOPIC, 0, 0L, "v1", event);

        assertEquals(occurredAtMillis, extractor.extract(record, 999_999L));
    }

    @Test
    void fallsBackToNonNegativePartitionTime() {
        final ConsumerRecord<Object, Object> tombstone =
                new ConsumerRecord<>(RollupTopology.INPUT_TOPIC, 0, 0L, "v1", null);
        assertEquals(500L, extractor.extract(tombstone, 500L));
        assertEquals(0L, extractor.extract(tombstone, -1L));
    }
}
