import type { Anomaly, AnomalyKind, VehicleEvent } from './model.js';

export interface VehicleMovedPayload {
  region: string;
  vehicleMoved: VehicleEvent;
}

export interface AnomalyDetectedPayload {
  region: string;
  anomalyDetected: Anomaly;
}

function requireRecord(value: unknown, eventName: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Decoded ${eventName} value is not a record`);
  }
  return value as Record<string, unknown>;
}

function requireString(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`Decoded field ${field} is not a non-empty string`);
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

function requireInteger(record: Record<string, unknown>, field: string): number {
  const value = requireNumber(record, field);
  if (!Number.isInteger(value)) {
    throw new Error(`Decoded field ${field} is not an integer`);
  }
  return value;
}

function requireDate(record: Record<string, unknown>, field: string): Date {
  const value = record[field];
  let date: Date;

  if (value instanceof Date) {
    date = new Date(value.getTime());
  } else if (typeof value === 'number') {
    date = new Date(value);
  } else if (typeof value === 'string') {
    date = /^-?\d+$/.test(value) ? new Date(Number(value)) : new Date(value);
  } else {
    throw new Error(`Decoded field ${field} is not a timestamp`);
  }

  if (!Number.isFinite(date.getTime())) {
    throw new Error(`Decoded field ${field} is not a valid timestamp`);
  }
  return date;
}

function requireAnomalyKind(record: Record<string, unknown>): AnomalyKind {
  const kind = requireString(record, 'kind');
  if (kind !== 'SPEED_THRESHOLD' && kind !== 'SPEED_ZSCORE') {
    throw new Error(`Decoded field kind has unsupported value ${kind}`);
  }
  return kind;
}

export function mapVehicleEvent(value: unknown): VehicleMovedPayload {
  const record = requireRecord(value, 'vehicle event');
  const region = requireString(record, 'region');

  return {
    region,
    vehicleMoved: {
      eventId: requireString(record, 'event_id'),
      vehicleId: requireString(record, 'vehicle_id'),
      lat: requireNumber(record, 'lat'),
      lon: requireNumber(record, 'lon'),
      speedKph: requireNumber(record, 'speed_kph'),
      occurredAt: requireDate(record, 'occurred_at'),
    },
  };
}

export function mapAnomaly(value: unknown): AnomalyDetectedPayload {
  const record = requireRecord(value, 'anomaly');
  const region = requireString(record, 'region');

  return {
    region,
    anomalyDetected: {
      id: requireString(record, 'anomaly_id'),
      vehicleId: requireString(record, 'vehicle_id'),
      region,
      kind: requireAnomalyKind(record),
      value: requireNumber(record, 'value'),
      detectorVersion: requireInteger(record, 'detector_version'),
      windowStart: requireDate(record, 'window_start'),
      windowEnd: requireDate(record, 'window_end'),
    },
  };
}
