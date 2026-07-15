package com.gridpulse.anomaly;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Deterministic anomaly identifiers.
 *
 * <p>{@code anomaly_id} is a name-based UUID (version 5, SHA-1) of
 * {@code "{vehicle_id}|{window_start}|SPEED_THRESHOLD"} under a fixed namespace. Because it is a
 * pure function of the window key, replays and reprocessing produce identical ids and the M05
 * projector can upsert idempotently by this primary key. M08's z-score detector reuses this exact
 * namespace with kind {@code SPEED_ZSCORE}, so the namespace is a shared committed constant — never
 * inline it.
 */
public final class AnomalyIds {

    /** Fixed GridPulse anomaly-id namespace. Must stay stable forever — changing it re-keys every anomaly. */
    public static final UUID ANOMALY_NAMESPACE = UUID.fromString("a3c9f2e1-8b47-4d6a-bc19-5e2f7d0a1c34");

    private AnomalyIds() {
    }

    /** UUIDv5 for a SPEED_THRESHOLD anomaly in the window starting at {@code windowStartMillis}. */
    public static UUID thresholdAnomalyId(String vehicleId, long windowStartMillis) {
        return nameUuidV5(ANOMALY_NAMESPACE, vehicleId + "|" + windowStartMillis + "|SPEED_THRESHOLD");
    }

    /** RFC 4122 version-5 (SHA-1, name-based) UUID. {@link UUID#nameUUIDFromBytes} is v3 (MD5), so it is unusable here. */
    public static UUID nameUuidV5(UUID namespace, String name) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is required for UUIDv5 but unavailable", e);
        }
        sha1.update(toBytes(namespace));
        sha1.update(name.getBytes(StandardCharsets.UTF_8));
        final byte[] hash = Arrays.copyOf(sha1.digest(), 16);
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50); // version 5
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80); // IETF variant
        return fromBytes(hash);
    }

    private static byte[] toBytes(UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static UUID fromBytes(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
