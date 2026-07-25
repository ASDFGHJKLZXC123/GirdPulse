import { DateTimeResolver } from 'graphql-scalars';

import type { EventBus, EventPayloads } from './event-bus.js';
import { createLoaders, type RequestLoaders } from './loaders.js';
import {
  mapAnomaly,
  mapRegionRollup,
  mapVehicle,
  normalizeDateTime,
  type AnomalyRow,
  type Database,
  type DateTimeInput,
  type RegionRollupRow,
  type Vehicle,
  type VehicleRow,
} from './model.js';

export interface ApiContext extends Record<string, unknown> {
  database: Database;
  eventBus: EventBus;
  loaders: RequestLoaders;
}

interface RegionArgument {
  region?: string | null;
}

interface VehicleArgument {
  id: string;
}

interface AnomaliesArguments extends RegionArgument {
  since?: DateTimeInput | null;
}

interface RegionRollupsArguments {
  region: string;
  from: DateTimeInput;
  to: DateTimeInput;
}

interface RecentEventsArguments {
  limit: number;
}

interface VehicleAnomaliesArguments {
  since?: DateTimeInput | null;
}

async function* filterRegion<Value extends { region: string }>(
  source: AsyncIterable<Value>,
  region: string | null | undefined,
): AsyncGenerator<Value> {
  for await (const value of source) {
    if (region === null || region === undefined || value.region === region) {
      yield value;
    }
  }
}

export function createRequestContext(database: Database, eventBus: EventBus): ApiContext {
  return {
    database,
    eventBus,
    loaders: createLoaders(database),
  };
}

export function createResolvers() {
  return {
    DateTime: DateTimeResolver,
    Query: {
      vehicles: async (_root: unknown, { region }: RegionArgument, { database }: ApiContext) => {
        const result =
          region !== null && region !== undefined
            ? await database.query<VehicleRow>(
                `
                SELECT id, region, status, last_seen
                FROM vehicles
                WHERE region = $1
                ORDER BY id
              `,
                [region],
              )
            : await database.query<VehicleRow>(
                `
                SELECT id, region, status, last_seen
                FROM vehicles
                ORDER BY id
              `,
              );
        return result.rows.map(mapVehicle);
      },
      vehicle: async (_root: unknown, { id }: VehicleArgument, { database }: ApiContext) => {
        const result = await database.query<VehicleRow>(
          `
            SELECT id, region, status, last_seen
            FROM vehicles
            WHERE id = $1
          `,
          [id],
        );
        const row = result.rows[0];
        return row ? mapVehicle(row) : null;
      },
      anomalies: async (
        _root: unknown,
        { region, since: rawSince }: AnomaliesArguments,
        { database }: ApiContext,
      ) => {
        const since = normalizeDateTime(rawSince);
        const result = await database.query<AnomalyRow>(
          `
            SELECT id, vehicle_id, region, kind, value, detector_version,
                   window_start, window_end
            FROM anomalies
            WHERE ($1::text IS NULL OR region = $1)
              AND ($2::timestamptz IS NULL OR window_start >= $2)
            ORDER BY window_start DESC, id DESC
            LIMIT 500
          `,
          [region ?? null, since],
        );
        return result.rows.map(mapAnomaly);
      },
      regionRollups: async (
        _root: unknown,
        { region, from: rawFrom, to: rawTo }: RegionRollupsArguments,
        { database }: ApiContext,
      ) => {
        const from = normalizeDateTime(rawFrom);
        const to = normalizeDateTime(rawTo);
        const result = await database.query<RegionRollupRow>(
          `
            SELECT region, window_start, window_end, event_count,
                   active_vehicles, avg_speed_kph
            FROM region_rollups
            WHERE region = $1
              AND window_start >= $2
              AND window_start <= $3
            ORDER BY window_start ASC
          `,
          [region, from, to],
        );
        return result.rows.map(mapRegionRollup);
      },
    },
    Vehicle: {
      position: (vehicle: Vehicle, _args: unknown, { loaders }: ApiContext) =>
        loaders.position.load(vehicle.id),
      recentEvents: (vehicle: Vehicle, { limit }: RecentEventsArguments, { loaders }: ApiContext) =>
        loaders.recentEvents(limit).load(vehicle.id),
      anomalies: (
        vehicle: Vehicle,
        { since }: VehicleAnomaliesArguments,
        { loaders }: ApiContext,
      ) => loaders.anomalies(since).load(vehicle.id),
    },
    Subscription: {
      vehicleMoved: {
        subscribe: (_root: unknown, { region }: RegionArgument, { eventBus }: ApiContext) =>
          filterRegion(eventBus.subscribe('VEHICLE_MOVED'), region),
        resolve: (payload: EventPayloads['VEHICLE_MOVED']) => payload.vehicleMoved,
      },
      anomalyDetected: {
        subscribe: (_root: unknown, { region }: RegionArgument, { eventBus }: ApiContext) =>
          filterRegion(eventBus.subscribe('ANOMALY_DETECTED'), region),
        resolve: (payload: EventPayloads['ANOMALY_DETECTED']) => payload.anomalyDetected,
      },
    },
  };
}
