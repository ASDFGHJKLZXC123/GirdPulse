package com.gridpulse.anomaly;

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
    void returnsOccurredAtAsEventTime() {
        long occurredAtMillis = 1_700_000_123_456L;
        VehicleEvent event = VehicleEvent.newBuilder()
                .setEventId(UUID.randomUUID())
                .setVehicleId("v1").setRegion("west").setLat(1).setLon(2)
                .setSpeedKph(42.0).setHeadingDeg(0)
                .setStatus(VehicleStatus.ACTIVE)
                .setOccurredAt(Instant.ofEpochMilli(occurredAtMillis))
                .build();
        // partitionTime deliberately differs from occurred_at: proves event time is used, not ingestion time.
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("fleet.vehicle-events", 0, 0L, "v1", event);

        assertEquals(occurredAtMillis, extractor.extract(record, 999_999L));
    }

    @Test
    void fallsBackToPartitionTimeForNonEventValue() {
        ConsumerRecord<Object, Object> tombstone = new ConsumerRecord<>("fleet.vehicle-events", 0, 0L, "v1", null);
        assertEquals(500L, extractor.extract(tombstone, 500L));
    }

    @Test
    void fallsBackToZeroWhenNoStreamTimeYet() {
        ConsumerRecord<Object, Object> tombstone = new ConsumerRecord<>("fleet.vehicle-events", 0, 0L, "v1", null);
        assertEquals(0L, extractor.extract(tombstone, -1L));
    }
}
