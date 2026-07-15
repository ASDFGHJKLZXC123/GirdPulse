package com.gridpulse.anomaly;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Compact binary serde for {@link ViolationAggregate}, used for the windowed aggregate state store
 * and its changelog. Hand-rolled to avoid pulling a JSON dependency into the runtime for a
 * three-field internal value.
 */
public final class ViolationAggregateSerde implements Serde<ViolationAggregate> {

    @Override
    public Serializer<ViolationAggregate> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(data.violationCount());
                out.writeDouble(data.maxSpeed());
                out.writeUTF(data.lastRegion() == null ? "" : data.lastRegion());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return bytes.toByteArray();
        };
    }

    @Override
    public Deserializer<ViolationAggregate> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                final long violationCount = in.readLong();
                final double maxSpeed = in.readDouble();
                final String lastRegion = in.readUTF();
                return new ViolationAggregate(violationCount, maxSpeed, lastRegion);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
