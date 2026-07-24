package com.gridpulse.rollup;

import com.gridpulse.events.VehicleEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/** Uses the event's {@code occurred_at} field as Kafka Streams event time. */
public final class OccurredAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final Object value = record.value();
        if (value instanceof VehicleEvent event && event.getOccurredAt() != null) {
            return event.getOccurredAt().toEpochMilli();
        }
        return partitionTime < 0 ? 0L : partitionTime;
    }
}
