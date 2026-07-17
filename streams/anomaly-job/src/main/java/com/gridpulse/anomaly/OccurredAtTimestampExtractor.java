package com.gridpulse.anomaly;

import com.gridpulse.events.VehicleEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event-time extractor: windowing keys off {@code occurred_at} carried in the event, not the
 * broker/ingestion timestamp. This is what makes late-but-in-grace events land in the right window.
 */
public final class OccurredAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final Object value = record.value();
        if (value instanceof VehicleEvent event && event.getOccurredAt() != null) {
            return event.getOccurredAt().toEpochMilli();
        }
        // Tombstones / non-VehicleEvent payloads carry no event time; reuse the highest stream time
        // seen so Streams never rejects a negative timestamp.
        return partitionTime < 0 ? 0L : partitionTime;
    }
}
