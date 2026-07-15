import { describe, expect, it } from 'vitest';
import {
  loadConfig,
  REGION_CODES,
  SimulatorEngine,
} from './simulator-core';
import type { VehicleEvent } from './simulator-core';

const BASE_CONFIG_ENV = {
  BROKERS: 'localhost:9092',
  SCHEMA_REGISTRY_URL: 'http://localhost:8081',
  VEHICLES: '50',
  TICK_MS: '1000',
  SEED: '42',
  INJECT: '',
};

function configWith(overrides: Partial<ReturnType<typeof loadConfig>>): ReturnType<typeof loadConfig> {
  return {
    ...loadConfig(BASE_CONFIG_ENV),
    ...overrides,
  };
}

type TrackedEvent = {
  event: VehicleEvent;
  tick: number;
  tickMs: number;
};

function collectTrackedEvents(
  config: ReturnType<typeof loadConfig>,
  totalEvents: number,
  rng?: () => number,
): TrackedEvent[] {
  const engine = new SimulatorEngine(config, rng);
  const events: TrackedEvent[] = [];
  let tick = 0;
  while (events.length < totalEvents) {
    const nowMs = 1_000 + tick * config.tickMs;
    const produced = engine.tick(nowMs).events;
    for (const event of produced) {
      if (!event) {
        continue;
      }
      if (events.length >= totalEvents) {
        break;
      }
      events.push({ event, tick, tickMs: nowMs });
    }
    tick += 1;
  }
  return events;
}

class SequenceRng {
  private index = 0;
  constructor(private values: number[]) {}

  next = () => {
    const value = this.values[this.index];
    this.index += 1;
    return value === undefined ? 0.5 : value;
  };
}

type RngPlanByTick = Map<number, number[]>;

function createTickAwareRng(plan: RngPlanByTick): { setTick: (tick: number) => void; next: () => number } {
  let currentTick = 0;
  let callInTick = 0;
  return {
    setTick: (tick: number) => {
      currentTick = tick;
      callInTick = 0;
    },
    next: () => {
      const bucket = plan.get(currentTick) ?? [];
      const value = bucket[callInTick];
      callInTick += 1;
      return value ?? 0.5;
    },
  };
}

describe('simulator config', () => {
  it('honors defaults and parses env', () => {
    const config = loadConfig({});
    expect(config).toEqual({
      brokers: ['localhost:9092'],
      schemaRegistryUrl: 'http://localhost:8081',
      vehicles: 50,
      tickMs: 1000,
      seed: '42',
      inject: new Set(),
    });
  });
});

describe('fleet model', () => {
  it('assigns vehicle IDs and regions round-robin', () => {
    const config = configWith({ vehicles: 8, seed: 'ids' });
    const engine = new SimulatorEngine(config);
    const ids = engine.vehicles.map((vehicle) => vehicle.vehicleId);
    const regions = engine.vehicles.map((vehicle) => vehicle.region.code);
    expect(ids).toEqual(['veh-0001', 'veh-0002', 'veh-0003', 'veh-0004', 'veh-0005', 'veh-0006', 'veh-0007', 'veh-0008']);
    expect(regions).toEqual(['SEA', 'SFO', 'NYC', 'AUS', 'SEA', 'SFO', 'NYC', 'AUS']);
  });

  it('updates movement within bounds and reflects at edges', () => {
    const config = configWith({ vehicles: 1, seed: 'reflect', tickMs: 1000 });
    const rng = new SequenceRng([0.5, 0.5, 0.5, 0.5]);
    const engine = new SimulatorEngine(config, rng.next);
    const vehicle = engine.vehicles[0];
    vehicle.region = REGION_CODES[0];
    vehicle.lat = vehicle.region.latMax - 0.00001;
    vehicle.lon = (vehicle.region.lonMin + vehicle.region.lonMax) / 2;
    vehicle.headingDeg = 10;
    vehicle.speedKph = 80;

    const { events } = engine.tick(1_000);
    const event = events[0];

    expect(event.speed_kph).toBeLessThanOrEqual(80);
    expect(event.lat).toBeLessThanOrEqual(vehicle.region.latMax);
    expect(event.lat).toBeGreaterThanOrEqual(vehicle.region.latMin);
    expect(event.lon).toBeLessThanOrEqual(vehicle.region.lonMax);
    expect(event.lon).toBeGreaterThanOrEqual(vehicle.region.lonMin);
    expect(event.heading_deg).toBeCloseTo(350, 3);
  });

  it('applies transition before movement and emits zero speed on ACTIVE->IDLE transition', () => {
    const rng = new SequenceRng([0.5, 0.5, 0.5, 0.5, 0.01]);
    const config = configWith({ vehicles: 1, seed: 'order' });
    const engine = new SimulatorEngine(config, rng.next);
    const { events } = engine.tick(1_000);
    expect(events).toHaveLength(1);
    expect(events[0].status).toBe('IDLE');
    expect(events[0].speed_kph).toBe(0);
  });
});

describe('probabilities and determinism', () => {
  it('keeps transition probabilities within tolerance across 20k observations', () => {
    const config = configWith({
      seed: 'probability',
      vehicles: 20,
      tickMs: 1,
    });
    const engine = new SimulatorEngine(config);

    let beforeActive = 0;
    let activeToIdle = 0;
    let activeToOffline = 0;
    let beforeIdle = 0;
    let idleToActive = 0;
    let beforeOffline = 0;
    let offlineToActive = 0;

    for (let tick = 0; tick < 20000; tick++) {
      const beforeStatuses = engine.vehicles.map((vehicle) => vehicle.status);
      engine.tick(1_000 + tick);
      const afterStatuses = engine.vehicles.map((vehicle) => vehicle.status);

      for (let i = 0; i < beforeStatuses.length; i++) {
        const before = beforeStatuses[i];
        const after = afterStatuses[i];
        if (before === 'ACTIVE') {
          beforeActive += 1;
          if (after === 'IDLE') {
            activeToIdle += 1;
          } else if (after === 'OFFLINE') {
            activeToOffline += 1;
          }
        } else if (before === 'IDLE') {
          beforeIdle += 1;
          if (after === 'ACTIVE') {
            idleToActive += 1;
          }
        } else if (before === 'OFFLINE') {
          beforeOffline += 1;
          if (after === 'ACTIVE') {
            offlineToActive += 1;
          }
        }
      }
    }

    expect(beforeActive).toBeGreaterThan(1000);
    expect(activeToIdle / beforeActive).toBeGreaterThan(0.01);
    expect(activeToIdle / beforeActive).toBeLessThan(0.03);

    expect(beforeActive).toBeGreaterThan(1000);
    expect(activeToOffline / beforeActive).toBeGreaterThan(0.002);
    expect(activeToOffline / beforeActive).toBeLessThan(0.008);

    expect(beforeIdle).toBeGreaterThan(100);
    expect(idleToActive / beforeIdle).toBeGreaterThan(0.05);
    expect(idleToActive / beforeIdle).toBeLessThan(0.15);

    expect(beforeOffline).toBeGreaterThan(100);
    expect(offlineToActive / beforeOffline).toBeGreaterThan(0.02);
    expect(offlineToActive / beforeOffline).toBeLessThan(0.08);
  }, 20_000);

  it('is deterministic for the same seed and different for different seeds', () => {
    const configA = configWith({ seed: '42', vehicles: 10, tickMs: 500 });
    const configB = configWith({ seed: '42', vehicles: 10, tickMs: 500 });
    const configC = configWith({ seed: '314', vehicles: 10, tickMs: 500 });

    const run = (cfg: typeof configA): string[] =>
      collectTrackedEvents(cfg, 500).map((entry) => {
        const e = entry.event;
        return `${e.vehicle_id},${e.lat.toFixed(8)},${e.lon.toFixed(8)},${e.speed_kph.toFixed(5)}`;
      });

    expect(run(configA)).toEqual(run(configB));
    expect(run(configA)).not.toEqual(run(configC));
  });
});

describe('injection semantics', () => {
  it('dupes: ~1% duplicate events', () => {
    const config = configWith({
      seed: 'dupes',
      vehicles: 40,
      inject: new Set(['dupes']),
    });
    const events = collectTrackedEvents(config, 12000).map((entry) => entry.event);
    const uniqueByEventId = new Map<string, number>();
    for (const event of events) {
      uniqueByEventId.set(event.event_id, (uniqueByEventId.get(event.event_id) ?? 0) + 1);
    }
    const duplicates = [...uniqueByEventId.values()].reduce((acc, count) => acc + (count - 1), 0);
    const base = uniqueByEventId.size;
    const rate = duplicates / base;
    expect(rate).toBeGreaterThan(0.002);
    expect(rate).toBeLessThan(0.02);
  });

  it('late: ~2% events shifted 2-10 minutes to the past', () => {
    const config = configWith({
      seed: 'late',
      vehicles: 30,
      inject: new Set(['late']),
      tickMs: 1000,
    });
    const engine = new SimulatorEngine(config);
    const events: Array<{ event: VehicleEvent; tickIndex: number }> = [];
    let tick = 0;
    while (events.length < 12000) {
      const nowMs = 1_000 + tick * config.tickMs;
      const batch = engine.tick(nowMs).events;
      batch.forEach((event) => {
        events.push({ event, tickIndex: tick });
      });
      tick += 1;
    }

    const lateness = events.map(({ event, tickIndex }) => 1_000 + tickIndex * config.tickMs - event.occurred_at);
    const lateCount = lateness.filter((delta) => delta >= 2 * 60_000 && delta <= 10 * 60_000).length;
    const lateRate = lateCount / events.length;
    expect(lateRate).toBeGreaterThan(0.012);
    expect(lateRate).toBeLessThan(0.028);
  });

  it('spikes: applies 150-300 for five ticks every ceiling(60000/tickMs)', () => {
    const constantRng = new SequenceRng([]);
    const intervalConfig = configWith({
      seed: 'spikes-1000',
      vehicles: 1,
      inject: new Set(['spikes']),
      tickMs: 1000,
    });
    const intervalEngine = new SimulatorEngine(intervalConfig, constantRng.next);
    const interval = Math.ceil(60000 / intervalConfig.tickMs);
    const events1000: Array<{ speed: number; tick: number }> = [];
    for (let tick = 0; tick < 12_000; tick++) {
      const nowMs = 1_000 + tick * intervalConfig.tickMs;
      const [event] = intervalEngine.tick(nowMs).events;
      events1000.push({ speed: event.speed_kph, tick });
    }

    for (let tick = 0; tick < 12_000; tick++) {
      const inSpikeWindow = Math.floor(tick / interval) >= 1 && tick % interval < 5 && tick >= interval;
      if (inSpikeWindow) {
        expect(events1000[tick].speed).toBeGreaterThanOrEqual(150);
        expect(events1000[tick].speed).toBeLessThanOrEqual(300);
      } else {
        expect(events1000[tick].speed).toBeGreaterThanOrEqual(20);
        expect(events1000[tick].speed).toBeLessThanOrEqual(80);
      }
    }

    const fastRng = new SequenceRng([]);
    const fastConfig = configWith({
      seed: 'spikes-500',
      vehicles: 1,
      inject: new Set(['spikes']),
      tickMs: 500,
    });
    const fastEngine = new SimulatorEngine(fastConfig, fastRng.next);
    const fastInterval = Math.ceil(60000 / fastConfig.tickMs);
    const fastEvents: Array<{ speed: number; tick: number }> = [];
    for (let tick = 0; tick < 12_000; tick++) {
      const nowMs = 1_000 + tick * fastConfig.tickMs;
      const [event] = fastEngine.tick(nowMs).events;
      fastEvents.push({ speed: event.speed_kph, tick });
    }

    for (let tick = 0; tick < 12_000; tick++) {
      const inSpikeWindow = Math.floor(tick / fastInterval) >= 1 && tick % fastInterval < 5 && tick >= fastInterval;
      if (inSpikeWindow) {
        expect(fastEvents[tick].speed).toBeGreaterThanOrEqual(150);
        expect(fastEvents[tick].speed).toBeLessThanOrEqual(300);
      } else {
        expect(fastEvents[tick].speed).toBeGreaterThanOrEqual(20);
        expect(fastEvents[tick].speed).toBeLessThanOrEqual(80);
      }
    }
  });

  it('spikes defer transition for 5 ticks and resume on tick 6', () => {
    const config = configWith({
      seed: 'spikes-deferred',
      vehicles: 1,
      inject: new Set(['spikes']),
      tickMs: 1000,
    });
    const rng = createTickAwareRng(
      new Map([
        // At tick 61, a 0.01 first random would force ACTIVE->IDLE if transition was evaluated.
        [61, [0.01]],
        // At tick 65, resume transition logic and force ACTIVE->IDLE.
        [65, [0.01]],
      ]),
    );
    const engine = new SimulatorEngine(config, rng.next);

    const eventsByTick: VehicleEvent[] = [];
    for (let tick = 0; tick < 66; tick++) {
      rng.setTick(tick);
      const nowMs = 1_000 + tick * config.tickMs;
      const [event] = engine.tick(nowMs).events;
      eventsByTick.push(event);
    }

    const first = eventsByTick[60];
    const tail = eventsByTick[64];

    for (let tick = 60; tick < 65; tick += 1) {
      const event = eventsByTick[tick];
      expect(event.status).toBe('ACTIVE');
      expect(event.speed_kph).toBeGreaterThanOrEqual(150);
      expect(event.speed_kph).toBeLessThanOrEqual(300);
      if (tick > 60) {
        const previous = eventsByTick[tick - 1];
        const delta = Math.abs(event.lat - previous.lat) + Math.abs(event.lon - previous.lon);
        expect(delta).toBeGreaterThan(0);
      }
    }

    expect(first.speed_kph).toBeGreaterThanOrEqual(150);
    expect(tail.speed_kph).toBeGreaterThanOrEqual(150);

    const resumedTransitionEvent = eventsByTick[65];
    expect(resumedTransitionEvent.status).toBe('IDLE');
    expect(resumedTransitionEvent.speed_kph).toBe(0);
  });

  it('teleport: ~0.5% events jump to a new in-region position', () => {
    const config = configWith({
      seed: 'teleport',
      vehicles: 30,
      inject: new Set(['teleport']),
      tickMs: 1000,
    });
    const engine = new SimulatorEngine(config);
    const events: Array<{ event: VehicleEvent; tick: number }> = [];
    let tick = 0;
    while (events.length < 12000) {
      const nowMs = 1_000 + tick * config.tickMs;
      for (const event of engine.tick(nowMs).events) {
        events.push({ event, tick });
      }
      tick += 1;
    }

    const lastByVehicle = new Map<string, { lat: number; lon: number }>();
    let teleports = 0;
    for (const { event } of events) {
      const previous = lastByVehicle.get(event.vehicle_id);
      if (previous) {
        const dLat = Math.abs(event.lat - previous.lat);
        const dLon = Math.abs(event.lon - previous.lon);
        if (dLat > 0.01 || dLon > 0.01) {
          teleports += 1;
        }
      }
      lastByVehicle.set(event.vehicle_id, { lat: event.lat, lon: event.lon });
    }

    const rate = teleports / events.length;
    expect(rate).toBeGreaterThan(0.002);
    expect(rate).toBeLessThan(0.012);
  });

  it('no flags means clean baseline: no late/dupe/spike/teleport artifacts', () => {
    const config = configWith({
      seed: 'clean',
      vehicles: 30,
      inject: new Set(),
    });
    const events = collectTrackedEvents(config, 10000).map((entry) => entry.event);

    const uniqueIds = new Set(events.map((event) => event.event_id));
    const duplicateIds = events.length - uniqueIds.size;
    expect(uniqueIds.size).toBe(events.length);
    expect(duplicateIds).toBe(0);

    for (const event of events) {
      expect(event.speed_kph).toBeLessThanOrEqual(80);
    }
  });
});
