import type { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import {
  createM06ApiFixture,
  type M06ApiFixture,
} from '../../projector/test/support/m06-api-fixture.js';
import type { ApiConfig } from '../src/config.js';
import { startApi, type RunningApi, type SubscriptionTail } from '../src/server.js';

const ISO_8601 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

class NoopTail implements SubscriptionTail {
  readonly groupId = `gridpulse-api-subs-test-${process.pid}`;

  async start(): Promise<void> {}

  async stop(): Promise<void> {}
}

interface GraphqlResponse<T> {
  data?: T;
  errors?: Array<{ message: string }>;
}

let fixture: M06ApiFixture;
let api: RunningApi;

function testConfig(databaseUrl: string): ApiConfig {
  return {
    port: 0,
    databaseUrl,
    brokers: ['127.0.0.1:9092'],
    schemaRegistryUrl: 'http://127.0.0.1:8081',
  };
}

async function graphqlRequest<T>(source: string, variables?: Record<string, unknown>): Promise<T> {
  const response = await fetch(api.url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify({ query: source, variables }),
  });
  expect(response.status).toBe(200);

  const payload = (await response.json()) as GraphqlResponse<T>;
  expect(payload.errors).toBeUndefined();
  expect(payload.data).toBeDefined();
  return payload.data as T;
}

function iso(value: Date): string {
  return value.toISOString();
}

beforeAll(async () => {
  fixture = await createM06ApiFixture();
  api = await startApi({
    config: testConfig(fixture.databaseUrl),
    pool: fixture.pool as unknown as Pool,
    tail: new NoopTail(),
  });
}, 20_000);

afterAll(async () => {
  if (api) {
    await Promise.all([api.stop(), api.stop()]);
  }
  await fixture?.close();
});

describe.sequential('M06 PostgreSQL resolvers', () => {
  it('returns root resolver values that match direct PostgreSQL spot checks', async () => {
    const data = await graphqlRequest<{
      vehicles: Array<{
        id: string;
        region: string;
        status: string;
        lastSeen: string;
      }>;
      vehicle: {
        id: string;
        region: string;
        status: string;
        lastSeen: string;
      } | null;
      anomalies: Array<{
        id: string;
        vehicleId: string;
        region: string;
        kind: string;
        value: number;
        detectorVersion: number;
        windowStart: string;
        windowEnd: string;
      }>;
      regionRollups: Array<{
        region: string;
        windowStart: string;
        windowEnd: string;
        eventCount: number;
        activeVehicles: number;
        avgSpeedKph: number;
      }>;
    }>(`
      query RootResolverSpotChecks {
        vehicles(region: "SEA") {
          id
          region
          status
          lastSeen
        }
        vehicle(id: "veh-sfo-1") {
          id
          region
          status
          lastSeen
        }
        anomalies(region: "SEA", since: "2026-07-24T12:01:00.000Z") {
          id
          vehicleId
          region
          kind
          value
          detectorVersion
          windowStart
          windowEnd
        }
        regionRollups(
          region: "SEA"
          from: "2026-07-24T12:00:00.000Z"
          to: "2026-07-24T12:02:00.000Z"
        ) {
          region
          windowStart
          windowEnd
          eventCount
          activeVehicles
          avgSpeedKph
        }
      }
    `);

    const vehicles = await fixture.pool.query<{
      id: string;
      region: string;
      status: string;
      last_seen: Date;
    }>('SELECT id, region, status, last_seen FROM vehicles WHERE region = $1 ORDER BY id', ['SEA']);
    const vehicle = await fixture.pool.query<{
      id: string;
      region: string;
      status: string;
      last_seen: Date;
    }>('SELECT id, region, status, last_seen FROM vehicles WHERE id = $1', ['veh-sfo-1']);
    const anomalies = await fixture.pool.query<{
      id: string;
      vehicle_id: string;
      region: string;
      kind: string;
      value: number;
      detector_version: number;
      window_start: Date;
      window_end: Date;
    }>(
      `
        SELECT id, vehicle_id, region, kind, value, detector_version, window_start, window_end
        FROM anomalies
        WHERE region = $1 AND window_start >= $2
        ORDER BY window_start DESC
        LIMIT 500
      `,
      ['SEA', new Date('2026-07-24T12:01:00.000Z')],
    );
    const rollups = await fixture.pool.query<{
      region: string;
      window_start: Date;
      window_end: Date;
      event_count: string;
      active_vehicles: number;
      avg_speed_kph: number;
    }>(
      `
        SELECT region, window_start, window_end, event_count, active_vehicles, avg_speed_kph
        FROM region_rollups
        WHERE region = $1 AND window_start >= $2 AND window_start <= $3
        ORDER BY window_start ASC
      `,
      ['SEA', new Date('2026-07-24T12:00:00.000Z'), new Date('2026-07-24T12:02:00.000Z')],
    );

    expect([...data.vehicles].sort((left, right) => left.id.localeCompare(right.id))).toEqual(
      vehicles.rows.map((row) => ({
        id: row.id,
        region: row.region,
        status: row.status,
        lastSeen: iso(row.last_seen),
      })),
    );
    expect(data.vehicle).toEqual({
      id: vehicle.rows[0]?.id,
      region: vehicle.rows[0]?.region,
      status: vehicle.rows[0]?.status,
      lastSeen: iso(vehicle.rows[0]!.last_seen),
    });
    expect(data.anomalies).toEqual(
      anomalies.rows.map((row) => ({
        id: row.id,
        vehicleId: row.vehicle_id,
        region: row.region,
        kind: row.kind,
        value: row.value,
        detectorVersion: row.detector_version,
        windowStart: iso(row.window_start),
        windowEnd: iso(row.window_end),
      })),
    );
    expect(data.regionRollups).toEqual(
      rollups.rows.map((row) => ({
        region: row.region,
        windowStart: iso(row.window_start),
        windowEnd: iso(row.window_end),
        eventCount: Number(row.event_count),
        activeVehicles: row.active_vehicles,
        avgSpeedKph: row.avg_speed_kph,
      })),
    );

    for (const value of [
      ...data.vehicles.map((row) => row.lastSeen),
      data.vehicle?.lastSeen,
      ...data.anomalies.flatMap((row) => [row.windowStart, row.windowEnd]),
      ...data.regionRollups.flatMap((row) => [row.windowStart, row.windowEnd]),
    ]) {
      expect(value).toMatch(ISO_8601);
    }
  });

  it('batches the canonical nested query into no more than four SQL queries', async () => {
    const querySpy = vi.spyOn(fixture.pool, 'query');

    const data = await graphqlRequest<{
      vehicles: Array<{
        id: string;
        position: {
          lat: number;
          lon: number;
        };
        recentEvents: Array<{ speedKph: number }>;
        anomalies: Array<{ kind: string; value: number }>;
      }>;
    }>(`
      query CanonicalNestedQuery {
        vehicles(region: "SEA") {
          id
          position {
            lat
            lon
          }
          recentEvents(limit: 5) {
            speedKph
          }
          anomalies {
            kind
            value
          }
        }
      }
    `);

    const sqlQueryCount = querySpy.mock.calls.length;
    querySpy.mockRestore();

    expect(data.vehicles).toHaveLength(2);
    expect(data.vehicles.every((vehicle) => vehicle.position !== null)).toBe(true);
    expect(data.vehicles.find((vehicle) => vehicle.id === 'veh-sea-1')?.recentEvents).toHaveLength(
      3,
    );
    expect(sqlQueryCount).toBe(4);
  });

  it('keeps recentEvents limit and anomalies since values in loader identity', async () => {
    const data = await graphqlRequest<{
      vehicle: {
        newestEvent: Array<{ eventId: string; occurredAt: string }>;
        twoNewestEvents: Array<{ eventId: string; occurredAt: string }>;
        allAnomalies: Array<{ id: string; windowStart: string }>;
        laterAnomalies: Array<{ id: string; windowStart: string }>;
      };
    }>(`
      query LoaderArgumentIdentity {
        vehicle(id: "veh-sea-1") {
          newestEvent: recentEvents(limit: 1) {
            eventId
            occurredAt
          }
          twoNewestEvents: recentEvents(limit: 2) {
            eventId
            occurredAt
          }
          allAnomalies: anomalies(since: "2026-07-24T12:00:00.000Z") {
            id
            windowStart
          }
          laterAnomalies: anomalies(since: "2026-07-24T12:01:00.000Z") {
            id
            windowStart
          }
        }
      }
    `);

    expect(data.vehicle.newestEvent).toHaveLength(1);
    expect(data.vehicle.twoNewestEvents).toHaveLength(2);
    expect(data.vehicle.newestEvent[0]).toEqual(data.vehicle.twoNewestEvents[0]);
    expect(data.vehicle.allAnomalies).toHaveLength(2);
    expect(data.vehicle.laterAnomalies).toHaveLength(1);
    expect(data.vehicle.laterAnomalies[0]?.windowStart).toBe('2026-07-24T12:02:00.000Z');
  });
});
