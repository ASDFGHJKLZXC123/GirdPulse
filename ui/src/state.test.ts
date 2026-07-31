import { describe, expect, it } from 'vitest';

import {
  anomalousVehicleIds,
  latestRollup,
  mergeAnomalies,
  mergeVehicleRefresh,
  patchVehicleMovement,
  patchVehicleMovements,
  regionVariable,
  rollingRange,
} from './state.js';
import type { Anomaly, RegionRollup, Vehicle, VehicleEvent } from './types.js';

const NOW = new Date('2026-07-29T12:00:00.000Z');

function vehicle(id: string): Vehicle {
  return {
    id,
    region: 'SEA',
    status: 'ACTIVE',
    lastSeen: '2026-07-29T11:59:50.000Z',
    position: {
      lat: 47.6,
      lon: -122.34,
      speedKph: 20,
      headingDeg: 180,
      batteryPct: 61.5,
      updatedAt: '2026-07-29T11:59:50.000Z',
    },
  };
}

function movement(vehicleId: string): VehicleEvent {
  return {
    eventId: `event-${vehicleId}`,
    vehicleId,
    lat: 47.61,
    lon: -122.33,
    speedKph: 42,
    occurredAt: '2026-07-29T12:00:00.000Z',
  };
}

function anomaly(id: string, vehicleId: string, windowEnd: string): Anomaly {
  return {
    id,
    vehicleId,
    region: 'SEA',
    kind: 'SPEED_THRESHOLD',
    value: 130,
    detectorVersion: 1,
    windowStart: '2026-07-29T11:54:00.000Z',
    windowEnd,
  };
}

function rollup(windowEnd: string): RegionRollup {
  return {
    region: 'SEA',
    windowStart: '2026-07-29T11:59:00.000Z',
    windowEnd,
    eventCount: 25,
    activeVehicles: 4,
    avgSpeedKph: 38.5,
  };
}

describe('regionVariable', () => {
  it('maps the UI-only ALL option to a nullable GraphQL variable', () => {
    expect(regionVariable('ALL')).toBeNull();
    expect(regionVariable('NYC')).toBe('NYC');
  });
});

describe('patchVehicleMovement', () => {
  it('immutably patches only coordinates, speed, and position time', () => {
    const sea = vehicle('vehicle-sea');
    const other = vehicle('vehicle-other');
    const vehicles = [sea, other];

    const patched = patchVehicleMovement(vehicles, movement(sea.id));

    expect(patched).not.toBe(vehicles);
    expect(patched[0]).not.toBe(sea);
    expect(patched[1]).toBe(other);
    expect(patched[0]).toEqual({
      ...sea,
      position: {
        ...sea.position,
        lat: 47.61,
        lon: -122.33,
        speedKph: 42,
        updatedAt: '2026-07-29T12:00:00.000Z',
      },
    });
    expect(sea.position.speedKph).toBe(20);
  });

  it('preserves the existing battery, numeric or null, across a movement patch', () => {
    const charged = vehicle('vehicle-sea');
    const unknown = {
      ...vehicle('vehicle-sfo'),
      position: { ...vehicle('vehicle-sfo').position, batteryPct: null },
    };

    const patched = patchVehicleMovements(
      [charged, unknown],
      [movement(charged.id), movement(unknown.id)],
    );

    expect(patched[0]?.position.batteryPct).toBe(61.5);
    expect(patched[1]?.position.batteryPct).toBeNull();
  });

  it('preserves the array when the event has no loaded vehicle', () => {
    const vehicles = [vehicle('vehicle-sea')];
    expect(patchVehicleMovement(vehicles, movement('vehicle-sfo'))).toBe(vehicles);
  });

  it('preserves the array for a duplicate movement event', () => {
    const current = vehicle('vehicle-sea');
    const duplicate = {
      ...movement(current.id),
      lat: current.position.lat,
      lon: current.position.lon,
      speedKph: current.position.speedKph,
      occurredAt: current.position.updatedAt,
    };

    expect(patchVehicleMovement([current], duplicate)[0]).toBe(current);
  });

  it('ignores an out-of-order movement event', () => {
    const current = vehicle('vehicle-sea');
    const staleMovement = {
      ...movement(current.id),
      occurredAt: '2026-07-29T11:59:49.000Z',
    };

    expect(patchVehicleMovement([current], staleMovement)).toEqual([current]);
  });

  it('applies one latest event per vehicle from a buffered simulator burst', () => {
    const sea = vehicle('vehicle-sea');
    const sfo = vehicle('vehicle-sfo');
    const olderSea = {
      ...movement(sea.id),
      lat: 47.605,
      occurredAt: '2026-07-29T11:59:59.000Z',
    };
    const latestSea = movement(sea.id);
    const latestSfo = movement(sfo.id);

    const patched = patchVehicleMovements([sea, sfo], [olderSea, latestSea, latestSfo]);

    expect(patched[0]?.position.lat).toBe(latestSea.lat);
    expect(patched[1]?.position.lat).toBe(latestSfo.lat);
  });
});

describe('mergeVehicleRefresh', () => {
  it('refreshes status fields without replacing a newer subscription position', () => {
    const current = {
      ...vehicle('vehicle-sea'),
      position: {
        ...vehicle('vehicle-sea').position,
        lat: 47.7,
        updatedAt: '2026-07-29T12:00:05.000Z',
      },
    };
    const refreshed = {
      ...vehicle('vehicle-sea'),
      status: 'IDLE' as const,
      lastSeen: '2026-07-29T12:00:00.000Z',
    };

    expect(mergeVehicleRefresh([current], [refreshed])).toEqual([
      {
        ...refreshed,
        position: current.position,
      },
    ]);
  });

  it('uses a newer polled position and drops vehicles absent from the refreshed filter', () => {
    const stale = vehicle('vehicle-sea');
    const refreshed = {
      ...stale,
      position: {
        ...stale.position,
        lon: -122.3,
        updatedAt: '2026-07-29T12:00:01.000Z',
      },
    };

    expect(mergeVehicleRefresh([stale, vehicle('vehicle-old')], [refreshed])).toEqual([refreshed]);
  });

  it('keeps the newer local coordinates while accepting the refreshed battery', () => {
    const current = {
      ...vehicle('vehicle-sea'),
      position: {
        ...vehicle('vehicle-sea').position,
        lat: 47.7,
        updatedAt: '2026-07-29T12:00:05.000Z',
      },
    };
    const refreshed = {
      ...vehicle('vehicle-sea'),
      position: {
        ...vehicle('vehicle-sea').position,
        batteryPct: 42.25,
      },
    };

    expect(mergeVehicleRefresh([current], [refreshed])).toEqual([
      {
        ...refreshed,
        position: {
          ...current.position,
          batteryPct: 42.25,
        },
      },
    ]);
  });

  it('treats a battery-only difference as a change', () => {
    const current = vehicle('vehicle-sea');
    const refreshed = {
      ...current,
      position: { ...current.position, batteryPct: null },
    };

    const merged = mergeVehicleRefresh([current], [refreshed]);

    expect(merged[0]).not.toBe(current);
    expect(merged[0]?.position.batteryPct).toBeNull();
  });

  it('preserves state identity when a refresh contains no changes', () => {
    const current = [vehicle('vehicle-sea')];
    const refreshed = structuredClone(current);

    expect(mergeVehicleRefresh(current, refreshed)).toBe(current);
  });
});

describe('mergeAnomalies', () => {
  it('deduplicates by id, prefers incoming data, and sorts newest first', () => {
    const duplicate = anomaly('anomaly-1', 'vehicle-sea', '2026-07-29T11:57:00.000Z');
    const replacement = {
      ...duplicate,
      value: 145,
      windowEnd: '2026-07-29T11:59:00.000Z',
    };
    const newest = anomaly('anomaly-2', 'vehicle-sfo', '2026-07-29T12:00:00.000Z');

    expect(mergeAnomalies([duplicate], [newest, replacement])).toEqual([newest, replacement]);
  });

  it('retains every anomaly for map highlighting while sorting newest first', () => {
    const rows = Array.from({ length: 205 }, (_, index) =>
      anomaly(
        `anomaly-${index.toString().padStart(3, '0')}`,
        `vehicle-${index}`,
        new Date(NOW.getTime() - index * 1_000).toISOString(),
      ),
    );

    const merged = mergeAnomalies([], rows);

    expect(merged).toHaveLength(205);
    expect(merged[0]?.id).toBe('anomaly-000');
    expect(merged.at(-1)?.id).toBe('anomaly-204');
  });

  it('preserves state identity when incoming anomalies contain no changes', () => {
    const current = [anomaly('anomaly-1', 'vehicle-sea', '2026-07-29T12:00:00.000Z')];

    expect(mergeAnomalies(current, structuredClone(current))).toBe(current);
    expect(mergeAnomalies([], [])).toEqual([]);
  });
});

describe('anomalousVehicleIds', () => {
  it('uses windowEnd and includes only the trailing five-minute window', () => {
    const anomalies = [
      anomaly('at-cutoff', 'vehicle-cutoff', '2026-07-29T11:55:00.000Z'),
      anomaly('recent', 'vehicle-recent', '2026-07-29T11:59:59.000Z'),
      anomaly('old', 'vehicle-old', '2026-07-29T11:54:59.999Z'),
      anomaly('future', 'vehicle-future', '2026-07-29T12:00:00.001Z'),
    ];

    expect(anomalousVehicleIds(anomalies, NOW)).toEqual(
      new Set(['vehicle-cutoff', 'vehicle-recent']),
    );
  });
});

describe('rollingRange', () => {
  it('returns a fresh five-minute ISO range ending at the supplied time', () => {
    expect(rollingRange(NOW)).toEqual({
      from: '2026-07-29T11:55:00.000Z',
      to: '2026-07-29T12:00:00.000Z',
    });
  });
});

describe('latestRollup', () => {
  it('selects the greatest windowEnd without relying on input order', () => {
    const latest = rollup('2026-07-29T12:00:00.000Z');
    expect(
      latestRollup([
        latest,
        rollup('2026-07-29T11:58:00.000Z'),
        rollup('2026-07-29T11:59:00.000Z'),
      ]),
    ).toBe(latest);
    expect(latestRollup([])).toBeNull();
  });
});
