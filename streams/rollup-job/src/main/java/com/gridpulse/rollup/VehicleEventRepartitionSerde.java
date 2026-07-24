package com.gridpulse.rollup;

import com.gridpulse.events.VehicleEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Schema-local Avro serde used only for the internal region repartition topic.
 *
 * <p>A Confluent {@code SpecificAvroSerde} uses topic-name subjects. With production
 * {@code auto.register.schemas=false}, it correctly refuses to create a subject for the dynamically
 * named internal topic. The internal edge therefore uses the already pinned/generated v1 reader
 * schema directly, while the public input and output topics continue to use the canonical Schema
 * Registry subjects.
 */
public final class VehicleEventRepartitionSerde implements Serde<VehicleEvent> {

    @Override
    public Serializer<VehicleEvent> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try {
                final var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
                new SpecificDatumWriter<VehicleEvent>(VehicleEvent.getClassSchema())
                        .write(data, encoder);
                encoder.flush();
                return bytes.toByteArray();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @Override
    public Deserializer<VehicleEvent> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                final var decoder = DecoderFactory.get().binaryDecoder(data, null);
                return new SpecificDatumReader<VehicleEvent>(VehicleEvent.getClassSchema())
                        .read(null, decoder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
