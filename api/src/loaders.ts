import DataLoader from 'dataloader';

import {
  mapAnomaly,
  mapPosition,
  mapVehicleEvent,
  normalizeDateTime,
  type Anomaly,
  type AnomalyRow,
  type Database,
  type DateTimeInput,
  type Position,
  type PositionRow,
  type VehicleEvent,
  type VehicleEventRow,
} from './model.js';

export interface RequestLoaders {
  position: DataLoader<string, Position>;
  recentEventsByLimit: Map<number, DataLoader<string, VehicleEvent[]>>;
  anomaliesBySince: Map<string, DataLoader<string, Anomaly[]>>;
  recentEvents(limit: number): DataLoader<string, VehicleEvent[]>;
  anomalies(since?: DateTimeInput | null): DataLoader<string, Anomaly[]>;
}

function groupByVehicle<Value extends { vehicleId: string }>(
  keys: readonly string[],
  values: readonly Value[],
): Value[][] {
  const grouped = new Map<string, Value[]>();
  for (const value of values) {
    const current = grouped.get(value.vehicleId);
    if (current) {
      current.push(value);
    } else {
      grouped.set(value.vehicleId, [value]);
    }
  }
  return keys.map((key) => grouped.get(key) ?? []);
}

function validateLimit(limit: number): number {
  if (!Number.isInteger(limit) || limit < 0) {
    throw new Error('recentEvents limit must be a non-negative integer');
  }
  return limit;
}

export function createLoaders(database: Database): RequestLoaders {
  const position = new DataLoader<string, Position>(async (vehicleIds) => {
    const result = await database.query<PositionRow>(
      `
        SELECT vehicle_id, lat, lon, speed_kph, heading_deg, battery_pct, updated_at
        FROM vehicle_positions
        WHERE vehicle_id = ANY($1::text[])
      `,
      [[...vehicleIds]],
    );
    const byVehicle = new Map(
      result.rows.map((row) => [row.vehicle_id, mapPosition(row)] as const),
    );
    return vehicleIds.map(
      (vehicleId) =>
        byVehicle.get(vehicleId) ?? new Error(`No position exists for vehicle ${vehicleId}`),
    );
  });

  const recentEventsByLimit = new Map<number, DataLoader<string, VehicleEvent[]>>();
  const anomaliesBySince = new Map<string, DataLoader<string, Anomaly[]>>();

  const recentEvents = (rawLimit: number): DataLoader<string, VehicleEvent[]> => {
    const limit = validateLimit(rawLimit);
    const existing = recentEventsByLimit.get(limit);
    if (existing) {
      return existing;
    }

    const loader = new DataLoader<string, VehicleEvent[]>(async (vehicleIds) => {
      if (limit === 0) {
        return vehicleIds.map(() => []);
      }
      const result = await database.query<VehicleEventRow>(
        `
          WITH ranked_events AS (
            SELECT event_id, vehicle_id, lat, lon, speed_kph, occurred_at,
                   ROW_NUMBER() OVER (
                     PARTITION BY vehicle_id
                     ORDER BY occurred_at DESC, event_id DESC
                   ) AS event_rank
            FROM vehicle_events
            WHERE vehicle_id = ANY($1::text[])
          )
          SELECT event_id, vehicle_id, lat, lon, speed_kph, occurred_at
          FROM ranked_events
          WHERE event_rank <= $2
          ORDER BY vehicle_id, occurred_at DESC, event_id DESC
        `,
        [[...vehicleIds], limit],
      );
      return groupByVehicle(vehicleIds, result.rows.map(mapVehicleEvent));
    });
    recentEventsByLimit.set(limit, loader);
    return loader;
  };

  const anomalies = (rawSince?: DateTimeInput | null): DataLoader<string, Anomaly[]> => {
    const since = normalizeDateTime(rawSince);
    const identity = since?.toISOString() ?? 'all';
    const existing = anomaliesBySince.get(identity);
    if (existing) {
      return existing;
    }

    const loader = new DataLoader<string, Anomaly[]>(async (vehicleIds) => {
      const result = await database.query<AnomalyRow>(
        `
          SELECT id, vehicle_id, region, kind, value, detector_version,
                 window_start, window_end
          FROM anomalies
          WHERE vehicle_id = ANY($1::text[])
            AND ($2::timestamptz IS NULL OR window_start >= $2)
          ORDER BY vehicle_id, window_start DESC, id DESC
        `,
        [[...vehicleIds], since],
      );
      return groupByVehicle(vehicleIds, result.rows.map(mapAnomaly));
    });
    anomaliesBySince.set(identity, loader);
    return loader;
  };

  return {
    position,
    recentEventsByLimit,
    anomaliesBySince,
    recentEvents,
    anomalies,
  };
}
