package com.gridpulse.rollup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Binary state-store serde for {@link RollupAggregate}. */
public final class RollupAggregateSerde implements Serde<RollupAggregate> {

    @Override
    public Serializer<RollupAggregate> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(data.eventCount());
                out.writeDouble(data.activeSpeedSum());
                out.writeLong(data.activeSpeedCount());
                // Stable ordering keeps the serialized state deterministic across equivalent folds.
                final Set<String> orderedIds = new TreeSet<>(data.activeVehicleIds());
                out.writeInt(orderedIds.size());
                for (String vehicleId : orderedIds) {
                    out.writeUTF(vehicleId);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return bytes.toByteArray();
        };
    }

    @Override
    public Deserializer<RollupAggregate> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                final long eventCount = in.readLong();
                final double activeSpeedSum = in.readDouble();
                final long activeSpeedCount = in.readLong();
                final int activeVehicleCount = in.readInt();
                if (activeVehicleCount < 0) {
                    throw new IOException("negative active vehicle count in rollup state");
                }
                final Set<String> activeVehicleIds = new HashSet<>();
                for (int i = 0; i < activeVehicleCount; i++) {
                    activeVehicleIds.add(in.readUTF());
                }
                return new RollupAggregate(
                        eventCount, activeVehicleIds, activeSpeedSum, activeSpeedCount);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
