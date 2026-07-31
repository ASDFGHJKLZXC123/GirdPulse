import { describe, expect, it } from 'vitest';

import { VEHICLE_EVENTS_TOPIC, coerceDecodedEvent, type VehicleEvent } from '../src/types.js';

const REJECTION = 'Decoded field battery_pct is not a finite number or null';

const V1_RECORD = {
  event_id: '10000000-0000-4000-8000-000000000001',
  vehicle_id: 'veh-0001',
  region: 'SEA',
  lat: 47.6062,
  lon: -122.3321,
  speed_kph: 42.5,
  heading_deg: 90,
  status: 'ACTIVE',
  occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
};

function coerceBattery(battery_pct: unknown): number | null {
  const event = coerceDecodedEvent(VEHICLE_EVENTS_TOPIC, {
    ...V1_RECORD,
    battery_pct,
  }) as VehicleEvent;
  return event.battery_pct;
}

function expectRejected(battery_pct: unknown): void {
  expect(() => coerceBattery(battery_pct)).toThrow(REJECTION);
}

describe('M08c battery_pct normalization', () => {
  it('normalizes a v1 record without the field to null', () => {
    const event = coerceDecodedEvent(VEHICLE_EVENTS_TOPIC, V1_RECORD) as VehicleEvent;
    expect(event.battery_pct).toBeNull();
    expect(event).toEqual({ ...V1_RECORD, battery_pct: null });
  });

  it('normalizes an explicit v2 null to null', () => {
    expect(coerceBattery(null)).toBeNull();
  });

  it('keeps a finite numeric v2 value, including zero', () => {
    expect(coerceBattery(61.5)).toBe(61.5);
    expect(coerceBattery(0)).toBe(0);
  });

  it('rejects a numeric string rather than silently parsing it', () => {
    expectRejected('61.5');
  });

  it('rejects non-finite numbers', () => {
    expectRejected(Number.NaN);
    expectRejected(Number.POSITIVE_INFINITY);
  });

  it('rejects a wrapped Avro union object instead of an unwrapped double', () => {
    expectRejected({ double: 61.5 });
  });
});
