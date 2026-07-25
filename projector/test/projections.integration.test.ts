import { randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { runMigrations } from '../src/migrations.js';
import { applyAnomaly, applyRegionRollup, applyVehicleEvent } from '../src/projections.js';
import { applyRetention } from '../src/retention.js';
import { processDecodedBatch } from '../src/transactions.js';
import type { Anomaly, DecodedBatch, RegionRollup, VehicleEvent } from '../src/types.js';
import { VEHICLE_EVENTS_TOPIC } from '../src/types.js';

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgresql://gridpulse:gridpulse@127.0.0.1:5432/gridpulse';
const SCHEMA = `m05_test_${process.pid}_${Date.now()}`;
const PROJECTION_TABLES = [
  'vehicles',
  'vehicle_positions',
  'vehicle_events',
  'anomalies',
  'region_rollups',
  'projector_offsets',
] as const;

let adminPool: Pool;
let testPool: Pool;

function identifier(value: string): string {
  if (!/^[a-z][a-z0-9_]*$/.test(value)) {
    throw new Error(`Unsafe test identifier: ${value}`);
  }
  return `"${value}"`;
}

function table(name: (typeof PROJECTION_TABLES)[number]): string {
  return `${identifier(SCHEMA)}.${identifier(name)}`;
}

function vehicleEvent(overrides: Partial<VehicleEvent> = {}): VehicleEvent {
  return {
    event_id: randomUUID(),
    vehicle_id: 'veh-0001',
    region: 'SEA',
    lat: 47.6062,
    lon: -122.3321,
    speed_kph: 42.5,
    heading_deg: 90,
    status: 'ACTIVE',
    occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
    ...overrides,
  };
}

function anomaly(overrides: Partial<Anomaly> = {}): Anomaly {
  return {
    anomaly_id: randomUUID(),
    vehicle_id: 'veh-0001',
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

async function applyEvent(event: VehicleEvent, swapCoords = false): Promise<void> {
  const client = await testPool.connect();
  try {
    await applyVehicleEvent(client, event, { swapCoords });
  } finally {
    client.release();
  }
}

async function resetProjectionTables(): Promise<void> {
  const names = PROJECTION_TABLES.map((name) => table(name)).join(', ');
  await testPool.query(`TRUNCATE TABLE ${names}`);
}

beforeAll(async () => {
  adminPool = new Pool({
    connectionString: DATABASE_URL,
    application_name: 'gridpulse-m05-test-admin',
    max: 1,
  });
  await adminPool.query(`CREATE SCHEMA ${identifier(SCHEMA)}`);

  testPool = new Pool({
    connectionString: DATABASE_URL,
    application_name: 'gridpulse-m05-tests',
    options: `-c search_path=${SCHEMA}`,
    max: 2,
  });

  const searchPath = await testPool.query<{ current_schema: string }>('SELECT current_schema()');
  expect(searchPath.rows[0]?.current_schema).toBe(SCHEMA);
  await runMigrations(testPool);
}, 15_000);

afterEach(async () => {
  await resetProjectionTables();
});

afterAll(async () => {
  if (testPool) {
    await testPool.end();
  }
  if (adminPool) {
    await adminPool.query(`DROP SCHEMA IF EXISTS ${identifier(SCHEMA)} CASCADE`);
    await adminPool.end();
  }
});

describe.sequential('VehicleEvent projection', () => {
  it('creates the current vehicle, position, and immutable event history rows', async () => {
    const event = vehicleEvent();

    await applyEvent(event);

    const current = await testPool.query<{
      id: string;
      region: string;
      status: string;
      last_seen: Date;
      lat: number;
      lon: number;
      speed_kph: number;
      heading_deg: number;
      updated_at: Date;
    }>(`
      SELECT v.id, v.region, v.status, v.last_seen,
             p.lat, p.lon, p.speed_kph, p.heading_deg, p.updated_at
      FROM ${table('vehicles')} v
      JOIN ${table('vehicle_positions')} p ON p.vehicle_id = v.id
    `);
    const history = await testPool.query<{
      event_id: string;
      vehicle_id: string;
      lat: number;
      lon: number;
      speed_kph: number;
      occurred_at: Date;
    }>(`SELECT * FROM ${table('vehicle_events')}`);

    expect(current.rows).toHaveLength(1);
    expect(current.rows[0]).toMatchObject({
      id: event.vehicle_id,
      region: event.region,
      status: event.status,
      lat: event.lat,
      lon: event.lon,
      speed_kph: event.speed_kph,
      heading_deg: event.heading_deg,
    });
    expect(current.rows[0]?.last_seen.toISOString()).toBe('2026-07-24T12:00:00.000Z');
    expect(current.rows[0]?.updated_at.toISOString()).toBe('2026-07-24T12:00:00.000Z');
    expect(history.rows).toHaveLength(1);
    expect(history.rows[0]).toMatchObject({
      event_id: event.event_id,
      vehicle_id: event.vehicle_id,
      lat: event.lat,
      lon: event.lon,
      speed_kph: event.speed_kph,
    });
  });

  it('does not let late events regress current state or position and ignores duplicate event ids', async () => {
    const newest = vehicleEvent({
      region: 'NYC',
      status: 'IDLE',
      lat: 40.7128,
      lon: -74.006,
      speed_kph: 0,
      heading_deg: 180,
      occurred_at: Date.parse('2026-07-24T12:02:00.000Z'),
    });
    const late = vehicleEvent({
      region: 'AUS',
      status: 'OFFLINE',
      lat: 30.2672,
      lon: -97.7431,
      speed_kph: 12,
      heading_deg: 270,
      occurred_at: Date.parse('2026-07-24T12:01:00.000Z'),
    });

    await applyEvent(newest);
    await applyEvent(late);
    await applyEvent(newest);

    const vehicle = await testPool.query<{
      region: string;
      status: string;
      last_seen: Date;
    }>(`SELECT region, status, last_seen FROM ${table('vehicles')}`);
    const position = await testPool.query<{
      lat: number;
      lon: number;
      updated_at: Date;
    }>(`SELECT lat, lon, updated_at FROM ${table('vehicle_positions')}`);
    const history = await testPool.query<{ count: string }>(
      `SELECT count(*) FROM ${table('vehicle_events')}`,
    );

    expect(vehicle.rows[0]).toMatchObject({
      region: newest.region,
      status: newest.status,
    });
    expect(vehicle.rows[0]?.last_seen.toISOString()).toBe('2026-07-24T12:02:00.000Z');
    expect(position.rows[0]).toMatchObject({
      lat: newest.lat,
      lon: newest.lon,
    });
    expect(position.rows[0]?.updated_at.toISOString()).toBe('2026-07-24T12:02:00.000Z');
    expect(history.rows[0]?.count).toBe('2');
  });

  it('uses the specified >= boundary for vehicle state but a strict > boundary for position', async () => {
    const first = vehicleEvent();
    const sameTimestamp = vehicleEvent({
      region: 'SFO',
      status: 'OFFLINE',
      lat: 37.7749,
      lon: -122.4194,
      speed_kph: 0,
      heading_deg: 315,
    });

    await applyEvent(first);
    await applyEvent(sameTimestamp);

    const vehicle = await testPool.query<{
      region: string;
      status: string;
    }>(`SELECT region, status FROM ${table('vehicles')}`);
    const position = await testPool.query<{
      lat: number;
      lon: number;
      speed_kph: number;
      heading_deg: number;
    }>(`SELECT lat, lon, speed_kph, heading_deg FROM ${table('vehicle_positions')}`);

    expect(vehicle.rows[0]).toEqual({
      region: sameTimestamp.region,
      status: sameTimestamp.status,
    });
    expect(position.rows[0]).toEqual({
      lat: first.lat,
      lon: first.lon,
      speed_kph: first.speed_kph,
      heading_deg: first.heading_deg,
    });
  });

  it('BUG_SWAP_COORDS swaps only latitude and longitude in both position tables', async () => {
    const event = vehicleEvent({
      lat: 47.625,
      lon: -122.35,
      speed_kph: 54.5,
      heading_deg: 135,
      status: 'ACTIVE',
    });

    await applyEvent(event, true);

    const current = await testPool.query<{
      lat: number;
      lon: number;
      speed_kph: number;
      heading_deg: number;
    }>(`SELECT lat, lon, speed_kph, heading_deg FROM ${table('vehicle_positions')}`);
    const history = await testPool.query<{
      lat: number;
      lon: number;
      speed_kph: number;
    }>(`SELECT lat, lon, speed_kph FROM ${table('vehicle_events')}`);
    const vehicle = await testPool.query<{
      region: string;
      status: string;
    }>(`SELECT region, status FROM ${table('vehicles')}`);

    expect(current.rows[0]).toEqual({
      lat: event.lon,
      lon: event.lat,
      speed_kph: event.speed_kph,
      heading_deg: event.heading_deg,
    });
    expect(history.rows[0]).toEqual({
      lat: event.lon,
      lon: event.lat,
      speed_kph: event.speed_kph,
    });
    expect(vehicle.rows[0]).toEqual({
      region: event.region,
      status: event.status,
    });
  });
});

describe.sequential('idempotent derived-topic projections', () => {
  it('upserts an anomaly by its deterministic id', async () => {
    const id = randomUUID();
    const original = anomaly({ anomaly_id: id });
    const corrected = anomaly({
      anomaly_id: id,
      region: 'SFO',
      kind: 'SPEED_ZSCORE',
      value: 4.75,
      detector_version: 2,
      window_start: Date.parse('2026-07-24T12:01:00.000Z'),
      window_end: Date.parse('2026-07-24T12:02:00.000Z'),
    });
    const client = await testPool.connect();
    try {
      await applyAnomaly(client, original);
      await applyAnomaly(client, corrected);
    } finally {
      client.release();
    }

    const rows = await testPool.query<{
      id: string;
      vehicle_id: string;
      region: string;
      kind: string;
      value: number;
      detector_version: number;
      window_start: Date;
      window_end: Date;
    }>(`SELECT * FROM ${table('anomalies')}`);

    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0]).toMatchObject({
      id,
      vehicle_id: corrected.vehicle_id,
      region: corrected.region,
      kind: corrected.kind,
      value: corrected.value,
      detector_version: corrected.detector_version,
    });
    expect(rows.rows[0]?.window_start.toISOString()).toBe('2026-07-24T12:01:00.000Z');
    expect(rows.rows[0]?.window_end.toISOString()).toBe('2026-07-24T12:02:00.000Z');
  });

  it('upserts a rollup by region and window_start while preserving other windows', async () => {
    const original = regionRollup();
    const corrected = regionRollup({
      window_end: Date.parse('2026-07-24T12:01:30.000Z'),
      event_count: 781,
      active_vehicles: 12,
      avg_speed_kph: 48.125,
    });
    const nextWindow = regionRollup({
      window_start: Date.parse('2026-07-24T12:01:00.000Z'),
      window_end: Date.parse('2026-07-24T12:02:00.000Z'),
      event_count: 720,
      active_vehicles: 11,
      avg_speed_kph: 44.5,
    });
    const client = await testPool.connect();
    try {
      await applyRegionRollup(client, original);
      await applyRegionRollup(client, corrected);
      await applyRegionRollup(client, nextWindow);
    } finally {
      client.release();
    }

    const rows = await testPool.query<{
      region: string;
      event_count: string;
      active_vehicles: number;
      avg_speed_kph: number;
      window_start: Date;
      window_end: Date;
    }>(`SELECT * FROM ${table('region_rollups')} ORDER BY window_start ASC`);

    expect(rows.rows).toHaveLength(2);
    expect(rows.rows[0]).toMatchObject({
      region: corrected.region,
      event_count: String(corrected.event_count),
      active_vehicles: corrected.active_vehicles,
      avg_speed_kph: corrected.avg_speed_kph,
    });
    expect(rows.rows[0]?.window_end.toISOString()).toBe('2026-07-24T12:01:30.000Z');
    expect(rows.rows[1]).toMatchObject({
      region: nextWindow.region,
      event_count: String(nextWindow.event_count),
      active_vehicles: nextWindow.active_vehicles,
      avg_speed_kph: nextWindow.avg_speed_kph,
    });
  });
});

describe.sequential('event-history retention', () => {
  it('deletes history older than 24 hours and preserves recent events', async () => {
    const old = vehicleEvent({
      occurred_at: Date.now() - 25 * 60 * 60 * 1000,
    });
    const recent = vehicleEvent({
      occurred_at: Date.now() - 60 * 1000,
    });

    await applyEvent(old);
    await applyEvent(recent);

    expect(await applyRetention(testPool)).toBe(1);

    const remaining = await testPool.query<{
      event_id: string;
    }>(`SELECT event_id FROM ${table('vehicle_events')}`);
    expect(remaining.rows).toEqual([{ event_id: recent.event_id }]);
  });
});

describe.sequential('batch offset transaction', () => {
  it('stores the maximum offset represented by a decoded batch', async () => {
    const first = vehicleEvent({
      occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
    });
    const newest = vehicleEvent({
      occurred_at: Date.parse('2026-07-24T12:02:00.000Z'),
    });
    const late = vehicleEvent({
      occurred_at: Date.parse('2026-07-24T12:01:00.000Z'),
    });

    const result = await processDecodedBatch(
      testPool,
      {
        topic: VEHICLE_EVENTS_TOPIC,
        partition: 1,
        messages: [
          { offset: '7', value: first },
          { offset: '10', value: newest },
          { offset: '9', value: late },
        ],
      },
      { swapCoords: false },
    );
    const offset = await testPool.query<{ last_offset: string }>(
      `SELECT last_offset FROM ${table('projector_offsets')} WHERE topic = $1 AND partition = 1`,
      [VEHICLE_EVENTS_TOPIC],
    );

    expect(result).toEqual({ processed: 3, lastOffset: '10' });
    expect(offset.rows[0]?.last_offset).toBe('10');

    const staleResult = await processDecodedBatch(
      testPool,
      {
        topic: VEHICLE_EVENTS_TOPIC,
        partition: 1,
        messages: [{ offset: '8', value: first }],
      },
      { swapCoords: false },
    );
    expect(staleResult).toEqual({ processed: 1, lastOffset: '10' });
  });

  it('rolls back projection writes and the offset together when a fault is injected', async () => {
    const committed = vehicleEvent({
      status: 'ACTIVE',
      lat: 47.61,
      lon: -122.33,
      occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
    });
    const interrupted = vehicleEvent({
      status: 'IDLE',
      lat: 47.62,
      lon: -122.34,
      speed_kph: 0,
      occurred_at: Date.parse('2026-07-24T12:01:00.000Z'),
    });
    const batch: DecodedBatch = {
      topic: VEHICLE_EVENTS_TOPIC,
      partition: 2,
      messages: [{ offset: '10', value: committed }],
    };

    await processDecodedBatch(testPool, batch, { swapCoords: false });

    await expect(
      processDecodedBatch(
        testPool,
        {
          ...batch,
          messages: [{ offset: '11', value: interrupted }],
        },
        {
          swapCoords: false,
          faultAfterProjections: () => {
            throw new Error('injected failure before offset update');
          },
        },
      ),
    ).rejects.toThrow('injected failure before offset update');

    const afterFault = await testPool.query<{
      status: string;
      lat: number;
      lon: number;
      last_offset: string;
      history_count: string;
    }>(`
      SELECT v.status, p.lat, p.lon, o.last_offset,
             (SELECT count(*) FROM ${table('vehicle_events')}) AS history_count
      FROM ${table('vehicles')} v
      JOIN ${table('vehicle_positions')} p ON p.vehicle_id = v.id
      JOIN ${table('projector_offsets')} o
        ON o.topic = 'fleet.vehicle-events' AND o.partition = 2
    `);

    expect(afterFault.rows[0]).toEqual({
      status: committed.status,
      lat: committed.lat,
      lon: committed.lon,
      last_offset: '10',
      history_count: '1',
    });

    await processDecodedBatch(
      testPool,
      {
        ...batch,
        messages: [{ offset: '11', value: interrupted }],
      },
      { swapCoords: false },
    );

    const afterRetry = await testPool.query<{
      status: string;
      lat: number;
      lon: number;
      last_offset: string;
      history_count: string;
    }>(`
      SELECT v.status, p.lat, p.lon, o.last_offset,
             (SELECT count(*) FROM ${table('vehicle_events')}) AS history_count
      FROM ${table('vehicles')} v
      JOIN ${table('vehicle_positions')} p ON p.vehicle_id = v.id
      JOIN ${table('projector_offsets')} o
        ON o.topic = 'fleet.vehicle-events' AND o.partition = 2
    `);

    expect(afterRetry.rows[0]).toEqual({
      status: interrupted.status,
      lat: interrupted.lat,
      lon: interrupted.lon,
      last_offset: '11',
      history_count: '2',
    });
  });
});
