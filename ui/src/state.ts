import type {
  Anomaly,
  DateTimeRange,
  RegionCode,
  RegionRollup,
  RegionSelection,
  Vehicle,
  VehicleEvent,
} from './types.js';

const FIVE_MINUTES_MS = 5 * 60 * 1_000;

function sameVehicle(left: Vehicle, right: Vehicle): boolean {
  return (
    left.id === right.id &&
    left.region === right.region &&
    left.status === right.status &&
    left.lastSeen === right.lastSeen &&
    left.position.lat === right.position.lat &&
    left.position.lon === right.position.lon &&
    left.position.speedKph === right.position.speedKph &&
    left.position.headingDeg === right.position.headingDeg &&
    left.position.batteryPct === right.position.batteryPct &&
    left.position.updatedAt === right.position.updatedAt
  );
}

function sameAnomaly(left: Anomaly, right: Anomaly): boolean {
  return (
    left.id === right.id &&
    left.vehicleId === right.vehicleId &&
    left.region === right.region &&
    left.kind === right.kind &&
    left.value === right.value &&
    left.detectorVersion === right.detectorVersion &&
    left.windowStart === right.windowStart &&
    left.windowEnd === right.windowEnd
  );
}

export function regionVariable(selection: RegionSelection): RegionCode | null {
  return selection === 'ALL' ? null : selection;
}

export function patchVehicleMovements(
  vehicles: Vehicle[],
  events: readonly VehicleEvent[],
): Vehicle[] {
  const latestByVehicle = new Map<string, VehicleEvent>();
  for (const event of events) {
    const buffered = latestByVehicle.get(event.vehicleId);
    if (!buffered || Date.parse(event.occurredAt) >= Date.parse(buffered.occurredAt)) {
      latestByVehicle.set(event.vehicleId, event);
    }
  }

  let changed = false;
  const nextVehicles = vehicles.map((vehicle) => {
    const event = latestByVehicle.get(vehicle.id);
    if (!event || Date.parse(event.occurredAt) < Date.parse(vehicle.position.updatedAt)) {
      return vehicle;
    }
    if (
      event.occurredAt === vehicle.position.updatedAt &&
      event.lat === vehicle.position.lat &&
      event.lon === vehicle.position.lon &&
      event.speedKph === vehicle.position.speedKph
    ) {
      return vehicle;
    }

    changed = true;
    return {
      ...vehicle,
      position: {
        ...vehicle.position,
        lat: event.lat,
        lon: event.lon,
        speedKph: event.speedKph,
        updatedAt: event.occurredAt,
      },
    };
  });

  return changed ? nextVehicles : vehicles;
}

export function patchVehicleMovement(vehicles: Vehicle[], event: VehicleEvent): Vehicle[] {
  return patchVehicleMovements(vehicles, [event]);
}

export function mergeVehicleRefresh(current: Vehicle[], refreshed: readonly Vehicle[]): Vehicle[] {
  const currentById = new Map(current.map((vehicle) => [vehicle.id, vehicle]));

  const merged = refreshed.map((vehicle) => {
    const existing = currentById.get(vehicle.id);
    let candidate = vehicle;

    if (
      existing &&
      Date.parse(existing.position.updatedAt) > Date.parse(vehicle.position.updatedAt)
    ) {
      // The subscription only carries coordinates, so keep the newer local fix
      // but take batteryPct from the refresh, which is the only source for it.
      candidate = {
        ...vehicle,
        position: {
          ...existing.position,
          batteryPct: vehicle.position.batteryPct,
        },
      };
    }

    return existing && sameVehicle(existing, candidate) ? existing : candidate;
  });

  return merged.length === current.length &&
    merged.every((vehicle, index) => vehicle === current[index])
    ? current
    : merged;
}

export function mergeAnomalies(
  current: Anomaly[],
  incoming: Anomaly | readonly Anomaly[],
): Anomaly[] {
  const byId = new Map(current.map((anomaly) => [anomaly.id, anomaly]));
  const additions = Array.isArray(incoming) ? incoming : [incoming];

  for (const anomaly of additions) {
    const existing = byId.get(anomaly.id);
    if (!existing || !sameAnomaly(existing, anomaly)) {
      byId.set(anomaly.id, anomaly);
    }
  }

  const merged = [...byId.values()].sort((left, right) => {
    const timeDifference = Date.parse(right.windowEnd) - Date.parse(left.windowEnd);
    return timeDifference || right.id.localeCompare(left.id);
  });

  return merged.length === current.length &&
    merged.every((anomaly, index) => anomaly === current[index])
    ? current
    : merged;
}

export function anomalousVehicleIds(
  anomalies: readonly Anomaly[],
  now: Date | number = Date.now(),
): Set<string> {
  const nowMs = typeof now === 'number' ? now : now.getTime();
  const cutoffMs = nowMs - FIVE_MINUTES_MS;

  return new Set(
    anomalies
      .filter((anomaly) => {
        const windowEndMs = Date.parse(anomaly.windowEnd);
        return windowEndMs >= cutoffMs && windowEndMs <= nowMs;
      })
      .map((anomaly) => anomaly.vehicleId),
  );
}

export function rollingRange(now: Date | number = Date.now()): DateTimeRange {
  const to = typeof now === 'number' ? new Date(now) : now;
  return {
    from: new Date(to.getTime() - FIVE_MINUTES_MS).toISOString(),
    to: to.toISOString(),
  };
}

export function latestRollup(rollups: readonly RegionRollup[]): RegionRollup | null {
  return (
    rollups.reduce<RegionRollup | null>((latest, candidate) => {
      if (latest === null || Date.parse(candidate.windowEnd) > Date.parse(latest.windowEnd)) {
        return candidate;
      }
      return latest;
    }, null) ?? null
  );
}
