package com.gridpulse.anomaly;

/**
 * Per-(vehicle, window) running aggregate over threshold-violating events.
 *
 * @param violationCount number of events with {@code speed_kph > 120.0} folded into this window
 * @param maxSpeed       highest observed speed (becomes the anomaly {@code value})
 * @param lastRegion     region of the most recent violating event (becomes the anomaly {@code region})
 */
public record ViolationAggregate(long violationCount, double maxSpeed, String lastRegion) {

    /** Empty accumulator. {@code maxSpeed = 0.0} is safe: only speeds > 120 are ever folded in. */
    public static ViolationAggregate empty() {
        return new ViolationAggregate(0L, 0.0, "");
    }

    /** Fold one violating event into the aggregate. */
    public ViolationAggregate add(double speedKph, String region) {
        return new ViolationAggregate(violationCount + 1, Math.max(maxSpeed, speedKph), region);
    }
}
