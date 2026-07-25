import type { QueryConfigValues, QueryResult, QueryResultRow } from 'pg';

export type VehicleStatus = 'ACTIVE' | 'IDLE' | 'OFFLINE';
export type AnomalyKind = 'SPEED_THRESHOLD' | 'SPEED_ZSCORE';
export type DateTimeInput = Date | string;

export interface Database {
  query<Row extends QueryResultRow = QueryResultRow, Values extends unknown[] = unknown[]>(
    text: string,
    values?: QueryConfigValues<Values>,
  ): Promise<QueryResult<Row>>;
}

export interface Vehicle {
  id: string;
  region: string;
  status: VehicleStatus;
  lastSeen: Date;
}

export interface Position {
  lat: number;
  lon: number;
  speedKph: number;
  headingDeg: number;
  updatedAt: Date;
}

export interface VehicleEvent {
  eventId: string;
  vehicleId: string;
  lat: number;
  lon: number;
  speedKph: number;
  occurredAt: Date;
}

export interface VehicleMovement extends VehicleEvent {
  region: string;
}

export interface Anomaly {
  id: string;
  vehicleId: string;
  region: string;
  kind: AnomalyKind;
  value: number;
  detectorVersion: number;
  windowStart: Date;
  windowEnd: Date;
}

export interface RegionRollup {
  region: string;
  windowStart: Date;
  windowEnd: Date;
  eventCount: number;
  activeVehicles: number;
  avgSpeedKph: number;
}

export interface VehicleRow extends QueryResultRow {
  id: string;
  region: string;
  status: string;
  last_seen: Date | string;
}

export interface PositionRow extends QueryResultRow {
  vehicle_id: string;
  lat: number;
  lon: number;
  speed_kph: number;
  heading_deg: number;
  updated_at: Date | string;
}

export interface VehicleEventRow extends QueryResultRow {
  event_id: string;
  vehicle_id: string;
  lat: number;
  lon: number;
  speed_kph: number;
  occurred_at: Date | string;
}

export interface AnomalyRow extends QueryResultRow {
  id: string;
  vehicle_id: string;
  region: string;
  kind: string;
  value: number;
  detector_version: number;
  window_start: Date | string;
  window_end: Date | string;
}

export interface RegionRollupRow extends QueryResultRow {
  region: string;
  window_start: Date | string;
  window_end: Date | string;
  event_count: number | string;
  active_vehicles: number;
  avg_speed_kph: number;
}

export function toDate(value: Date | string, fieldName: string): Date {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) {
    throw new Error(`Invalid ${fieldName}: ${String(value)}`);
  }
  return date;
}

export function normalizeDateTime(value: DateTimeInput | null | undefined): Date | null {
  return value === null || value === undefined ? null : toDate(value, 'DateTime');
}

function toVehicleStatus(value: string): VehicleStatus {
  if (value === 'ACTIVE' || value === 'IDLE' || value === 'OFFLINE') {
    return value;
  }
  throw new Error(`Invalid vehicle status: ${value}`);
}

function toAnomalyKind(value: string): AnomalyKind {
  if (value === 'SPEED_THRESHOLD' || value === 'SPEED_ZSCORE') {
    return value;
  }
  throw new Error(`Invalid anomaly kind: ${value}`);
}

function toGraphqlInt(value: number | string, fieldName: string): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < -2_147_483_648 || parsed > 2_147_483_647) {
    throw new Error(`${fieldName} is outside the GraphQL Int range`);
  }
  return parsed;
}

export function mapVehicle(row: VehicleRow): Vehicle {
  return {
    id: row.id,
    region: row.region,
    status: toVehicleStatus(row.status),
    lastSeen: toDate(row.last_seen, 'vehicles.last_seen'),
  };
}

export function mapPosition(row: PositionRow): Position {
  return {
    lat: row.lat,
    lon: row.lon,
    speedKph: row.speed_kph,
    headingDeg: row.heading_deg,
    updatedAt: toDate(row.updated_at, 'vehicle_positions.updated_at'),
  };
}

export function mapVehicleEvent(row: VehicleEventRow): VehicleEvent {
  return {
    eventId: row.event_id,
    vehicleId: row.vehicle_id,
    lat: row.lat,
    lon: row.lon,
    speedKph: row.speed_kph,
    occurredAt: toDate(row.occurred_at, 'vehicle_events.occurred_at'),
  };
}

export function mapAnomaly(row: AnomalyRow): Anomaly {
  return {
    id: row.id,
    vehicleId: row.vehicle_id,
    region: row.region,
    kind: toAnomalyKind(row.kind),
    value: row.value,
    detectorVersion: row.detector_version,
    windowStart: toDate(row.window_start, 'anomalies.window_start'),
    windowEnd: toDate(row.window_end, 'anomalies.window_end'),
  };
}

export function mapRegionRollup(row: RegionRollupRow): RegionRollup {
  return {
    region: row.region,
    windowStart: toDate(row.window_start, 'region_rollups.window_start'),
    windowEnd: toDate(row.window_end, 'region_rollups.window_end'),
    eventCount: toGraphqlInt(row.event_count, 'region_rollups.event_count'),
    activeVehicles: toGraphqlInt(row.active_vehicles, 'region_rollups.active_vehicles'),
    avgSpeedKph: row.avg_speed_kph,
  };
}
