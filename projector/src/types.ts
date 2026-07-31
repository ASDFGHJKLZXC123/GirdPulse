export const VEHICLE_EVENTS_TOPIC = 'fleet.vehicle-events';
export const ANOMALIES_TOPIC = 'fleet.anomalies';
export const REGION_ROLLUPS_TOPIC = 'fleet.rollups.region-1m';

export const PROJECTOR_TOPICS = [
  VEHICLE_EVENTS_TOPIC,
  ANOMALIES_TOPIC,
  REGION_ROLLUPS_TOPIC,
] as const;

export type ProjectorTopic = (typeof PROJECTOR_TOPICS)[number];

export const PROJECTOR_PARTITION_COUNTS = {
  [VEHICLE_EVENTS_TOPIC]: 6,
  [ANOMALIES_TOPIC]: 6,
  [REGION_ROLLUPS_TOPIC]: 3,
} as const satisfies Record<ProjectorTopic, number>;

export type TimestampValue = number | string | Date;

export interface VehicleEvent {
  event_id: string;
  vehicle_id: string;
  region: string;
  lat: number;
  lon: number;
  speed_kph: number;
  heading_deg: number;
  battery_pct: number | null;
  status: string;
  occurred_at: TimestampValue;
}

export interface Anomaly {
  anomaly_id: string;
  vehicle_id: string;
  region: string;
  kind: string;
  value: number;
  detector_version: number;
  window_start: TimestampValue;
  window_end: TimestampValue;
}

export interface RegionRollup {
  region: string;
  window_start: TimestampValue;
  window_end: TimestampValue;
  event_count: number | string;
  active_vehicles: number;
  avg_speed_kph: number;
}

export type DecodedEvent = VehicleEvent | Anomaly | RegionRollup;

export interface DecodedBatchMessage {
  offset: string;
  value: DecodedEvent;
}

export interface DecodedBatch {
  topic: ProjectorTopic;
  partition: number;
  messages: DecodedBatchMessage[];
}

export function isProjectorTopic(topic: string): topic is ProjectorTopic {
  return PROJECTOR_TOPICS.some((candidate) => candidate === topic);
}

export function toDate(value: TimestampValue, fieldName: string): Date {
  const date =
    value instanceof Date ? value : new Date(typeof value === 'string' ? Number(value) : value);
  if (!Number.isFinite(date.getTime())) {
    throw new Error(`Invalid ${fieldName}: ${String(value)}`);
  }
  return date;
}

function requireRecord(value: unknown, topic: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Decoded ${topic} value is not a record`);
  }
  return value as Record<string, unknown>;
}

function requireString(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  if (typeof value !== 'string') {
    throw new Error(`Decoded field ${field} is not a string`);
  }
  return value;
}

function requireNumber(record: Record<string, unknown>, field: string): number {
  const value = record[field];
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`Decoded field ${field} is not a finite number`);
  }
  return value;
}

// v1 writers omit the field entirely and v2 writers may send an explicit null;
// both normalize to null so a mixed-version stream stays decodable.
function optionalNumber(record: Record<string, unknown>, field: string): number | null {
  const value = record[field];
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`Decoded field ${field} is not a finite number or null`);
  }
  return value;
}

function requireTimestamp(record: Record<string, unknown>, field: string): TimestampValue {
  const value = record[field];
  if (value instanceof Date || typeof value === 'number' || typeof value === 'string') {
    toDate(value, field);
    return value;
  }
  throw new Error(`Decoded field ${field} is not a timestamp`);
}

function requireLong(record: Record<string, unknown>, field: string): number | string {
  const value = record[field];
  if (
    (typeof value === 'number' && Number.isSafeInteger(value)) ||
    (typeof value === 'string' && /^-?\d+$/.test(value))
  ) {
    return value;
  }
  throw new Error(`Decoded field ${field} is not an integer`);
}

export function coerceDecodedEvent(topic: ProjectorTopic, value: unknown): DecodedEvent {
  const record = requireRecord(value, topic);

  if (topic === VEHICLE_EVENTS_TOPIC) {
    return {
      event_id: requireString(record, 'event_id'),
      vehicle_id: requireString(record, 'vehicle_id'),
      region: requireString(record, 'region'),
      lat: requireNumber(record, 'lat'),
      lon: requireNumber(record, 'lon'),
      speed_kph: requireNumber(record, 'speed_kph'),
      heading_deg: requireNumber(record, 'heading_deg'),
      battery_pct: optionalNumber(record, 'battery_pct'),
      status: requireString(record, 'status'),
      occurred_at: requireTimestamp(record, 'occurred_at'),
    };
  }

  if (topic === ANOMALIES_TOPIC) {
    return {
      anomaly_id: requireString(record, 'anomaly_id'),
      vehicle_id: requireString(record, 'vehicle_id'),
      region: requireString(record, 'region'),
      kind: requireString(record, 'kind'),
      value: requireNumber(record, 'value'),
      detector_version: requireNumber(record, 'detector_version'),
      window_start: requireTimestamp(record, 'window_start'),
      window_end: requireTimestamp(record, 'window_end'),
    };
  }

  return {
    region: requireString(record, 'region'),
    window_start: requireTimestamp(record, 'window_start'),
    window_end: requireTimestamp(record, 'window_end'),
    event_count: requireLong(record, 'event_count'),
    active_vehicles: requireNumber(record, 'active_vehicles'),
    avg_speed_kph: requireNumber(record, 'avg_speed_kph'),
  };
}
