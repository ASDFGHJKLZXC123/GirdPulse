import { describe, expect, it } from 'vitest';

import { createPopupContent } from './vehicle-popup.js';
import type { Vehicle } from '../types.js';

function vehicle(batteryPct: number | null): Vehicle {
  return {
    id: 'veh-0001',
    region: 'SEA',
    status: 'ACTIVE',
    lastSeen: '2026-07-29T12:00:00.000Z',
    position: {
      lat: 47.6,
      lon: -122.34,
      speedKph: 38,
      headingDeg: 90,
      batteryPct,
      updatedAt: '2026-07-29T12:00:00.000Z',
    },
  };
}

function batteryRow(batteryPct: number | null): string {
  const rows = [...createPopupContent(vehicle(batteryPct)).querySelectorAll('div')];
  const battery = rows.find((row) => row.textContent?.startsWith('Battery: '));
  if (!battery) {
    throw new Error('The popup does not render a Battery row');
  }
  return battery.textContent ?? '';
}

describe('createPopupContent battery row', () => {
  it('renders a numeric battery with one decimal and a percent sign', () => {
    expect(batteryRow(61.5)).toBe('Battery: 61.5%');
  });

  it('rounds to one decimal', () => {
    expect(batteryRow(61.44)).toBe('Battery: 61.4%');
    expect(batteryRow(61.46)).toBe('Battery: 61.5%');
    expect(batteryRow(100)).toBe('Battery: 100.0%');
  });

  it('renders zero rather than treating it as missing', () => {
    expect(batteryRow(0)).toBe('Battery: 0.0%');
  });

  it('renders Unknown for a null battery', () => {
    expect(batteryRow(null)).toBe('Battery: Unknown');
  });
});
