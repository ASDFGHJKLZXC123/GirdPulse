import { createRef, forwardRef } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { DashboardView } from './App.js';
import type { VehicleMapHandle } from './components/VehicleMap.js';
import type { Anomaly, RegionRollup, Vehicle } from './types.js';

vi.mock('./components/VehicleMap.js', () => ({
  VehicleMap: forwardRef<VehicleMapHandle>(function MockVehicleMap(_props, ref) {
    if (typeof ref === 'function') {
      ref({ flyToVehicle: vi.fn() });
    } else if (ref) {
      ref.current = { flyToVehicle: vi.fn() };
    }
    return <div data-testid="map-canvas" />;
  }),
}));

afterEach(cleanup);

const vehicle: Vehicle = {
  id: 'veh-0001',
  region: 'SEA',
  status: 'ACTIVE',
  lastSeen: '2026-07-29T12:00:00.000Z',
  position: {
    lat: 47.6,
    lon: -122.34,
    speedKph: 38,
    headingDeg: 90,
    batteryPct: 61.5,
    updatedAt: '2026-07-29T12:00:00.000Z',
  },
};

const anomaly: Anomaly = {
  id: 'anomaly-1',
  vehicleId: vehicle.id,
  region: 'SEA',
  kind: 'SPEED_THRESHOLD',
  value: 151,
  detectorVersion: 1,
  windowStart: '2026-07-29T11:55:00.000Z',
  windowEnd: '2026-07-29T12:00:00.000Z',
};

const rollup: RegionRollup = {
  region: 'SEA',
  windowStart: '2026-07-29T11:59:00.000Z',
  windowEnd: '2026-07-29T12:00:00.000Z',
  eventCount: 60,
  activeVehicles: 12,
  avgSpeedKph: 42.5,
};

describe('DashboardView', () => {
  it('renders the complete one-page contract without a live API', () => {
    const onRegionChange = vi.fn();
    const mapRef = createRef<VehicleMapHandle>();

    render(
      <DashboardView
        anomalies={[anomaly]}
        anomalyLoading={false}
        anomalousIds={new Set([vehicle.id])}
        mapRef={mapRef}
        onRegionChange={onRegionChange}
        rollupErrors={{}}
        rollupLoadingRegions={[]}
        rollups={{ SEA: rollup }}
        selectedRegion="ALL"
        vehicleLoading={false}
        vehicles={[vehicle]}
      />,
    );

    expect(screen.getByTestId('map-canvas')).toBeInTheDocument();
    expect(screen.getByTestId('anomaly-row')).toHaveAttribute('data-vehicle-id', vehicle.id);
    expect(screen.getByTestId('rollup-SEA')).toHaveTextContent('60');
    expect(screen.getByTestId('rollup-SFO')).toHaveTextContent('No recent rollup data');
    expect(screen.getByTestId('region-select')).toHaveValue('ALL');

    fireEvent.change(screen.getByTestId('region-select'), {
      target: { value: 'SEA' },
    });
    expect(onRegionChange).toHaveBeenCalledWith('SEA');
  });

  it('caps the rendered anomaly table at 200 rows', () => {
    const anomalies = Array.from({ length: 205 }, (_, index) => ({
      ...anomaly,
      id: `anomaly-${index}`,
      vehicleId: `veh-${index}`,
      windowEnd: new Date(Date.parse(anomaly.windowEnd) - index * 1_000).toISOString(),
    }));

    render(
      <DashboardView
        anomalies={anomalies}
        anomalyLoading={false}
        anomalousIds={new Set()}
        mapRef={createRef<VehicleMapHandle>()}
        onRegionChange={vi.fn()}
        rollupErrors={{}}
        rollupLoadingRegions={[]}
        rollups={{}}
        selectedRegion="ALL"
        vehicleLoading={false}
        vehicles={[]}
      />,
    );

    expect(screen.getAllByTestId('anomaly-row')).toHaveLength(200);
  });
});
