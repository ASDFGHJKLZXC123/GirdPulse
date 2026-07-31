import { randomUUID } from 'node:crypto';

import { Pool } from 'pg';

import { runMigrations } from '../../src/migrations.js';
import { applyAnomaly, applyRegionRollup, applyVehicleEvent } from '../../src/projections.js';
import type { Anomaly, RegionRollup, VehicleEvent } from '../../src/types.js';

const DEFAULT_DATABASE_URL = 'postgresql://gridpulse:gridpulse@127.0.0.1:5432/gridpulse';
const SCHEMA_PREFIX = 'm06_api_test';

let fixtureSequence = 0;

export interface M06ApiFixture {
  databaseUrl: string;
  pool: Pool;
  schema: string;
  close(): Promise<void>;
}

function identifier(value: string): string {
  if (!/^[a-z][a-z0-9_]*$/.test(value)) {
    throw new Error(`Unsafe test identifier: ${value}`);
  }
  return `"${value}"`;
}

function schemaConnectionString(connectionString: string, schema: string): string {
  const url = new URL(connectionString);
  url.searchParams.set('options', `-c search_path=${schema}`);
  return url.toString();
}

function vehicleEvent(overrides: Partial<VehicleEvent> = {}): VehicleEvent {
  return {
    event_id: randomUUID(),
    vehicle_id: 'veh-sea-1',
    region: 'SEA',
    lat: 47.6062,
    lon: -122.3321,
    speed_kph: 42.5,
    heading_deg: 90,
    battery_pct: null,
    status: 'ACTIVE',
    occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
    ...overrides,
  };
}

function anomaly(overrides: Partial<Anomaly> = {}): Anomaly {
  return {
    anomaly_id: randomUUID(),
    vehicle_id: 'veh-sea-1',
    region: 'SEA',
    kind: 'SPEED_THRESHOLD',
    value: 91.25,
    detector_version: 1,
    window_start: Date.parse('2026-07-24T12:00:00.000Z'),
    window_end: Date.parse('2026-07-24T12:01:00.000Z'),
    ...overrides,
  };
}

function regionRollup(overrides: Partial<RegionRollup> = {}): RegionRollup {
  return {
    region: 'SEA',
    window_start: Date.parse('2026-07-24T12:00:00.000Z'),
    window_end: Date.parse('2026-07-24T12:01:00.000Z'),
    event_count: 780,
    active_vehicles: 13,
    avg_speed_kph: 47.75,
    ...overrides,
  };
}

async function seed(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    const events: VehicleEvent[] = [
      vehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000001',
        occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
        speed_kph: 41,
      }),
      vehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000002',
        occurred_at: Date.parse('2026-07-24T12:01:00.000Z'),
        speed_kph: 42,
      }),
      // The newest veh-sea-1 event is a v2 record; veh-sea-2 stays v1/null so the
      // API surfaces both a numeric and a null battery.
      vehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000003',
        occurred_at: Date.parse('2026-07-24T12:02:00.000Z'),
        speed_kph: 43,
        battery_pct: 61.5,
      }),
      vehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000004',
        vehicle_id: 'veh-sea-2',
        lat: 47.62,
        lon: -122.35,
        status: 'IDLE',
        occurred_at: Date.parse('2026-07-24T12:00:30.000Z'),
        speed_kph: 0,
      }),
      vehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000005',
        vehicle_id: 'veh-sfo-1',
        region: 'SFO',
        lat: 37.7749,
        lon: -122.4194,
        heading_deg: 180,
        occurred_at: Date.parse('2026-07-24T12:01:30.000Z'),
        speed_kph: 51,
      }),
    ];

    for (const event of events) {
      await applyVehicleEvent(client, event, { swapCoords: false });
    }

    const anomalies: Anomaly[] = [
      anomaly({
        anomaly_id: '20000000-0000-4000-8000-000000000001',
        kind: 'SPEED_THRESHOLD',
        value: 91.25,
        window_start: Date.parse('2026-07-24T12:00:00.000Z'),
        window_end: Date.parse('2026-07-24T12:01:00.000Z'),
      }),
      anomaly({
        anomaly_id: '20000000-0000-4000-8000-000000000002',
        kind: 'SPEED_ZSCORE',
        value: 4.75,
        window_start: Date.parse('2026-07-24T12:02:00.000Z'),
        window_end: Date.parse('2026-07-24T12:03:00.000Z'),
      }),
      anomaly({
        anomaly_id: '20000000-0000-4000-8000-000000000003',
        vehicle_id: 'veh-sfo-1',
        region: 'SFO',
        value: 99,
        window_start: Date.parse('2026-07-24T12:01:00.000Z'),
        window_end: Date.parse('2026-07-24T12:02:00.000Z'),
      }),
    ];

    for (const record of anomalies) {
      await applyAnomaly(client, record);
    }

    const rollups: RegionRollup[] = [
      regionRollup(),
      regionRollup({
        window_start: Date.parse('2026-07-24T12:01:00.000Z'),
        window_end: Date.parse('2026-07-24T12:02:00.000Z'),
        event_count: 720,
        active_vehicles: 11,
        avg_speed_kph: 44.5,
      }),
      regionRollup({
        region: 'SFO',
        event_count: 640,
        active_vehicles: 9,
        avg_speed_kph: 50.25,
      }),
    ];

    for (const record of rollups) {
      await applyRegionRollup(client, record);
    }
  } finally {
    client.release();
  }
}

export async function createM06ApiFixture(
  connectionString = process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL,
): Promise<M06ApiFixture> {
  fixtureSequence += 1;
  const schema = `${SCHEMA_PREFIX}_${process.pid}_${Date.now()}_${fixtureSequence}`;
  const adminPool = new Pool({
    connectionString,
    application_name: 'gridpulse-m06-api-test-admin',
    max: 1,
  });
  const databaseUrl = schemaConnectionString(connectionString, schema);
  let pool: Pool | undefined;

  try {
    await adminPool.query(`CREATE SCHEMA ${identifier(schema)}`);
    pool = new Pool({
      connectionString: databaseUrl,
      application_name: 'gridpulse-m06-api-tests',
      max: 4,
    });
    await runMigrations(pool);
    await seed(pool);

    return {
      databaseUrl,
      pool,
      schema,
      async close(): Promise<void> {
        await pool?.end();
        await adminPool.query(`DROP SCHEMA IF EXISTS ${identifier(schema)} CASCADE`);
        await adminPool.end();
      },
    };
  } catch (error) {
    await pool?.end();
    await adminPool.query(`DROP SCHEMA IF EXISTS ${identifier(schema)} CASCADE`);
    await adminPool.end();
    throw error;
  }
}
