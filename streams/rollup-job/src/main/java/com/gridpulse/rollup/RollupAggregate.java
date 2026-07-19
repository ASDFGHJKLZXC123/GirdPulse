package com.gridpulse.rollup;

import com.gridpulse.events.VehicleEvent;
import com.gridpulse.events.VehicleStatus;
import java.util.HashSet;
import java.util.Set;

/**
 * Per-(region, minute) accumulator.
 *
 * <p>The distinct ACTIVE vehicle set is intentionally held in memory. This is the documented
 * scalability shortcut for the demonstration fleet (at most a few thousand vehicles); a larger
 * deployment would use an approximate cardinality structure or a separately materialized index.
 */
public record RollupAggregate(
        long eventCount,
        Set<String> activeVehicleIds,
        double activeSpeedSum,
        long activeSpeedCount) {

    public RollupAggregate {
        activeVehicleIds = Set.copyOf(activeVehicleIds);
    }

    public static RollupAggregate empty() {
        return new RollupAggregate(0L, Set.of(), 0.0, 0L);
    }

    /** Counts every event, but includes only ACTIVE events in cardinality and average speed. */
    public RollupAggregate add(VehicleEvent event) {
        if (event.getStatus() != VehicleStatus.ACTIVE) {
            return new RollupAggregate(
                    eventCount + 1, activeVehicleIds, activeSpeedSum, activeSpeedCount);
        }

        final Set<String> nextActiveVehicleIds = new HashSet<>(activeVehicleIds);
        nextActiveVehicleIds.add(event.getVehicleId());
        return new RollupAggregate(
                eventCount + 1,
                nextActiveVehicleIds,
                activeSpeedSum + event.getSpeedKph(),
                activeSpeedCount + 1);
    }

    public int activeVehicleCount() {
        return activeVehicleIds.size();
    }

    /** Required output is 0.0, never NaN, when the window has no ACTIVE events. */
    public double averageActiveSpeedKph() {
        return activeSpeedCount == 0 ? 0.0 : activeSpeedSum / activeSpeedCount;
    }
}
