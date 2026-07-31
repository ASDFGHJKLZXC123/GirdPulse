import type { PoolClient } from 'pg';
import {
  ANOMALIES_TOPIC,
  REGION_ROLLUPS_TOPIC,
  VEHICLE_EVENTS_TOPIC,
  type Anomaly,
  type DecodedEvent,
  type ProjectorTopic,
  type RegionRollup,
  type VehicleEvent,
  toDate,
} from './types.js';

export interface ProjectionOptions {
  swapCoords: boolean;
}

export async function applyVehicleEvent(
  client: PoolClient,
  event: VehicleEvent,
  options: ProjectionOptions,
): Promise<void> {
  const occurredAt = toDate(event.occurred_at, 'occurred_at');
  const lat = options.swapCoords ? event.lon : event.lat;
  const lon = options.swapCoords ? event.lat : event.lon;

  await client.query(
    `
      INSERT INTO vehicles (id, region, status, last_seen)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (id) DO UPDATE SET
        region = CASE
          WHEN EXCLUDED.last_seen >= vehicles.last_seen THEN EXCLUDED.region
          ELSE vehicles.region
        END,
        status = CASE
          WHEN EXCLUDED.last_seen >= vehicles.last_seen THEN EXCLUDED.status
          ELSE vehicles.status
        END,
        last_seen = GREATEST(vehicles.last_seen, EXCLUDED.last_seen)
    `,
    [event.vehicle_id, event.region, event.status, occurredAt],
  );

  await client.query(
    `
      INSERT INTO vehicle_positions
        (vehicle_id, lat, lon, speed_kph, heading_deg, battery_pct, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (vehicle_id) DO UPDATE SET
        lat = EXCLUDED.lat,
        lon = EXCLUDED.lon,
        speed_kph = EXCLUDED.speed_kph,
        heading_deg = EXCLUDED.heading_deg,
        battery_pct = EXCLUDED.battery_pct,
        updated_at = EXCLUDED.updated_at
      WHERE EXCLUDED.updated_at > vehicle_positions.updated_at
    `,
    [event.vehicle_id, lat, lon, event.speed_kph, event.heading_deg, event.battery_pct, occurredAt],
  );

  await client.query(
    `
      INSERT INTO vehicle_events
        (event_id, vehicle_id, lat, lon, speed_kph, occurred_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (event_id) DO NOTHING
    `,
    [event.event_id, event.vehicle_id, lat, lon, event.speed_kph, occurredAt],
  );
}

export async function applyAnomaly(client: PoolClient, anomaly: Anomaly): Promise<void> {
  await client.query(
    `
      INSERT INTO anomalies
        (id, vehicle_id, region, kind, value, detector_version, window_start, window_end)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (id) DO UPDATE SET
        vehicle_id = EXCLUDED.vehicle_id,
        region = EXCLUDED.region,
        kind = EXCLUDED.kind,
        value = EXCLUDED.value,
        detector_version = EXCLUDED.detector_version,
        window_start = EXCLUDED.window_start,
        window_end = EXCLUDED.window_end
    `,
    [
      anomaly.anomaly_id,
      anomaly.vehicle_id,
      anomaly.region,
      anomaly.kind,
      anomaly.value,
      anomaly.detector_version,
      toDate(anomaly.window_start, 'window_start'),
      toDate(anomaly.window_end, 'window_end'),
    ],
  );
}

export async function applyRegionRollup(client: PoolClient, rollup: RegionRollup): Promise<void> {
  await client.query(
    `
      INSERT INTO region_rollups
        (region, window_start, window_end, event_count, active_vehicles, avg_speed_kph)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (region, window_start) DO UPDATE SET
        window_end = EXCLUDED.window_end,
        event_count = EXCLUDED.event_count,
        active_vehicles = EXCLUDED.active_vehicles,
        avg_speed_kph = EXCLUDED.avg_speed_kph
    `,
    [
      rollup.region,
      toDate(rollup.window_start, 'window_start'),
      toDate(rollup.window_end, 'window_end'),
      rollup.event_count,
      rollup.active_vehicles,
      rollup.avg_speed_kph,
    ],
  );
}

export async function applyProjection(
  client: PoolClient,
  topic: ProjectorTopic,
  value: DecodedEvent,
  options: ProjectionOptions,
): Promise<void> {
  if (topic === VEHICLE_EVENTS_TOPIC) {
    await applyVehicleEvent(client, value as VehicleEvent, options);
    return;
  }
  if (topic === ANOMALIES_TOPIC) {
    await applyAnomaly(client, value as Anomaly);
    return;
  }
  if (topic === REGION_ROLLUPS_TOPIC) {
    await applyRegionRollup(client, value as RegionRollup);
  }
}
