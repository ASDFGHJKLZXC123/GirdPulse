import seedrandom from 'seedrandom';

export type VehicleStatus = 'ACTIVE' | 'IDLE' | 'OFFLINE';

export type InjectionFlag = 'dupes' | 'late' | 'spikes' | 'teleport';

export type InjectionSet = Set<InjectionFlag>;

type Env = Record<string, string | undefined>;

export interface SimulatorConfig {
  brokers: string[];
  schemaRegistryUrl: string;
  vehicles: number;
  tickMs: number;
  seed: string;
  inject: InjectionSet;
}

export interface RegionSpec {
  code: string;
  latMin: number;
  latMax: number;
  lonMin: number;
  lonMax: number;
}

export interface VehicleState {
  vehicleId: string;
  region: RegionSpec;
  lat: number;
  lon: number;
  speedKph: number;
  headingDeg: number;
  status: VehicleStatus;
}

export interface VehicleEvent {
  event_id: string;
  vehicle_id: string;
  region: string;
  lat: number;
  lon: number;
  speed_kph: number;
  heading_deg: number;
  status: VehicleStatus;
  occurred_at: number;
}

export interface SimulatorTickResult {
  events: VehicleEvent[];
}

const DEFAULT_BROKERS = 'localhost:9092';
const DEFAULT_SCHEMA_REGISTRY_URL = 'http://localhost:8081';
const DEFAULT_VEHICLES = 50;
const DEFAULT_TICK_MS = 1000;
const DEFAULT_SEED = '42';
const INJECTION_DEFAULT: InjectionSet = new Set();

export const REGION_CODES = [
  { code: 'SEA', latMin: 47.5, latMax: 47.7, lonMin: -122.44, lonMax: -122.24 },
  { code: 'SFO', latMin: 37.7, latMax: 37.82, lonMin: -122.52, lonMax: -122.35 },
  { code: 'NYC', latMin: 40.68, latMax: 40.88, lonMin: -74.05, lonMax: -73.85 },
  { code: 'AUS', latMin: 30.2, latMax: 30.4, lonMin: -97.85, lonMax: -97.65 },
] as const;

const SPEED_MIN = 20;
const SPEED_MAX = 80;
const HEADING_DRIFT_MAX_DEG = 15;
const SPEED_DELTA_MAX_KPH = 5;
const LAT_PER_KM = 1 / 111;
const EARTH_COS_SCALE = 111;

const INJECT_DUPES_PCT = 0.01;
const INJECT_LATE_PCT = 0.02;
const INJECT_TELEPORT_PCT = 0.005;

const TRANSITION_ACTIVE_TO_IDLE = 0.02;
const TRANSITION_ACTIVE_TO_OFFLINE = 0.005;
const TRANSITION_IDLE_TO_ACTIVE = 0.1;
const TRANSITION_OFFLINE_TO_ACTIVE = 0.05;

const MIN_LATE_MS = 2 * 60 * 1000;
const MAX_LATE_MS = 10 * 60 * 1000;

function parseNumber(value: string | undefined, fallback: number, key: string): number {
  if (value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid ${key}: ${value}`);
  }
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${key}: must be a positive integer`);
  }
  return parsed;
}

function parseInjection(raw: string | undefined): InjectionSet {
  if (!raw) {
    return new Set(INJECTION_DEFAULT);
  }
  const set = new Set<InjectionFlag>();
  for (const token of raw.split(',')) {
    const normalized = token.trim().toLowerCase();
    if (!normalized) {
      continue;
    }
    if (normalized === 'dupes' || normalized === 'late' || normalized === 'spikes' || normalized === 'teleport') {
      set.add(normalized);
    }
  }
  return set;
}

function parseBrokers(raw: string | undefined): string[] {
  if (!raw || raw.trim() === '') {
    return [DEFAULT_BROKERS];
  }
  return raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

export function loadConfig(env: Env = process.env): SimulatorConfig {
  return {
    brokers: parseBrokers(env.BROKERS),
    schemaRegistryUrl: env.SCHEMA_REGISTRY_URL || DEFAULT_SCHEMA_REGISTRY_URL,
    vehicles: parseNumber(env.VEHICLES, DEFAULT_VEHICLES, 'VEHICLES'),
    tickMs: parseNumber(env.TICK_MS, DEFAULT_TICK_MS, 'TICK_MS'),
    seed: env.SEED || DEFAULT_SEED,
    inject: parseInjection(env.INJECT),
  };
}

function safeMod(angle: number): number {
  const normalized = angle % 360;
  return normalized < 0 ? normalized + 360 : normalized;
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

function uuidNibble(rng: () => number, length: number): string {
  return Array.from({ length })
    .map(() => Math.floor(rng() * 16).toString(16))
    .join('');
}

function randomUuidV4(rng: () => number): string {
  return `${uuidNibble(rng, 8)}-${uuidNibble(rng, 4)}-4${uuidNibble(rng, 3)}-${['8', '9', 'a', 'b'][Math.floor(rng() * 4)]}${uuidNibble(rng, 3)}-${uuidNibble(rng, 12)}`;
}

function randomUniform(rng: () => number, min: number, max: number): number {
  return min + (max - min) * rng();
}

function move(
  lat: number,
  lon: number,
  headingDeg: number,
  speedKph: number,
  tickMs: number,
): { lat: number; lon: number; headingDeg: number } {
  if (speedKph <= 0) {
    return { lat, lon, headingDeg };
  }

  const speedKmPerMs = speedKph / 3_600_000;
  const distanceKm = speedKmPerMs * tickMs;
  const headingRad = (headingDeg * Math.PI) / 180;

  const baseLat = lat + Math.cos(headingRad) * distanceKm * LAT_PER_KM;
  const regionScale = EARTH_COS_SCALE * Math.cos((lat * Math.PI) / 180);
  const baseLon = lon + Math.sin(headingRad) * distanceKm / Math.max(regionScale, 1e-12);

  return { lat: baseLat, lon: baseLon, headingDeg: headingDeg };
}

function reflectWithinBounds(
  lat: number,
  lon: number,
  headingDeg: number,
  region: RegionSpec,
): { lat: number; lon: number; headingDeg: number } {
  let currentLat = lat;
  let currentLon = lon;
  let heading = headingDeg;

  let stabilized = false;
  while (!stabilized) {
    stabilized = true;
    if (currentLat < region.latMin) {
      currentLat = region.latMin + (region.latMin - currentLat);
      heading = safeMod(360 - heading);
      stabilized = false;
    } else if (currentLat > region.latMax) {
      currentLat = region.latMax - (currentLat - region.latMax);
      heading = safeMod(360 - heading);
      stabilized = false;
    }

    if (currentLon < region.lonMin) {
      currentLon = region.lonMin + (region.lonMin - currentLon);
      heading = safeMod(180 - heading);
      stabilized = false;
    } else if (currentLon > region.lonMax) {
      currentLon = region.lonMax - (currentLon - region.lonMax);
      heading = safeMod(180 - heading);
      stabilized = false;
    }
  }

  return { lat: currentLat, lon: currentLon, headingDeg: heading };
}

function moveVehicle(
  state: VehicleState,
  tickMs: number,
  rng: () => number,
  forceSpiked: boolean,
): { speedKph: number; lat: number; lon: number; headingDeg: number } {
  if (state.status !== 'ACTIVE') {
    return {
      speedKph: 0,
      lat: state.lat,
      lon: state.lon,
      headingDeg: state.headingDeg,
    };
  }

  const heading = safeMod(state.headingDeg + randomUniform(rng, -HEADING_DRIFT_MAX_DEG, HEADING_DRIFT_MAX_DEG));
  let speed = randomUniform(rng, -SPEED_DELTA_MAX_KPH, SPEED_DELTA_MAX_KPH) + state.speedKph;
  speed = clamp(speed, SPEED_MIN, SPEED_MAX);

  if (forceSpiked) {
    const spikeSpeed = randomUniform(rng, 150, 300);
    const moved = move(state.lat, state.lon, heading, spikeSpeed, tickMs);
    const reflected = reflectWithinBounds(moved.lat, moved.lon, heading, state.region);
    return {
      speedKph: spikeSpeed,
      lat: reflected.lat,
      lon: reflected.lon,
      headingDeg: reflected.headingDeg,
    };
  }

  const moved = move(state.lat, state.lon, heading, speed, tickMs);
  const reflected = reflectWithinBounds(moved.lat, moved.lon, heading, state.region);
  return {
    speedKph: speed,
    lat: reflected.lat,
    lon: reflected.lon,
    headingDeg: reflected.headingDeg,
  };
}

export function buildInitialVehicles(config: SimulatorConfig, rng: () => number): VehicleState[] {
  return Array.from({ length: config.vehicles }, (_, index) => {
    const region = REGION_CODES[index % REGION_CODES.length];
    const vehicleId = `veh-${String(index + 1).padStart(4, '0')}`;
    return {
      vehicleId,
      region,
      lat: randomUniform(rng, region.latMin, region.latMax),
      lon: randomUniform(rng, region.lonMin, region.lonMax),
      speedKph: randomUniform(rng, SPEED_MIN, SPEED_MAX),
      headingDeg: randomUniform(rng, 0, 360),
      status: 'ACTIVE',
    };
  });
}

function applyTransition(state: VehicleState, rng: () => number): boolean {
  let transitioned = false;
  if (state.status === 'ACTIVE') {
    const roll = rng();
    if (roll < TRANSITION_ACTIVE_TO_IDLE) {
      state.status = 'IDLE';
      state.speedKph = 0;
      transitioned = true;
    } else if (roll < TRANSITION_ACTIVE_TO_IDLE + TRANSITION_ACTIVE_TO_OFFLINE) {
      state.status = 'OFFLINE';
      state.speedKph = 0;
      transitioned = true;
    }
    return transitioned;
  }

  if (state.status === 'IDLE') {
    if (rng() < TRANSITION_IDLE_TO_ACTIVE) {
      state.status = 'ACTIVE';
      state.speedKph = randomUniform(rng, SPEED_MIN, SPEED_MAX);
      transitioned = true;
    }
    return transitioned;
  }

  if (state.status === 'OFFLINE' && rng() < TRANSITION_OFFLINE_TO_ACTIVE) {
    state.status = 'ACTIVE';
    state.speedKph = randomUniform(rng, SPEED_MIN, SPEED_MAX);
    transitioned = true;
  }

  return transitioned;
}

function cloneState(state: VehicleState): VehicleState {
  return { ...state };
}

export function buildSeededRng(seed: string): () => number {
  return seedrandom(seed);
}

export class SimulatorEngine {
  private readonly config: SimulatorConfig;
  private readonly rng: () => number;
  private readonly spikeInterval: number;
  private tickIndex = 0;
  private readonly startedAtMs: number;
  readonly vehicles: VehicleState[];

  private spikeVehicleState: {
    vehicleId: string;
    remainingTicks: number;
  } | null = null;

  constructor(config: SimulatorConfig, rng?: () => number) {
    this.config = { ...config };
    this.rng = rng ?? buildSeededRng(this.config.seed);
    this.vehicles = buildInitialVehicles(this.config, this.rng);
    this.startedAtMs = Date.now();
    this.spikeInterval = Math.max(1, Math.ceil(60000 / this.config.tickMs));
  }

  private spawnEventId(): string {
    return randomUuidV4(this.rng);
  }

  private getRegions(): string {
    return REGION_CODES.map((entry) => entry.code).join(', ');
  }

  private shouldInject(flag: InjectionFlag): boolean {
    return this.config.inject.has(flag);
  }

  private maybeStartSpikes(): void {
    if (!this.shouldInject('spikes')) {
      return;
    }
    if (this.tickIndex === 0 || this.tickIndex % this.spikeInterval !== 0) {
      return;
    }
    const activeVehicles = this.vehicles.filter((vehicle) => vehicle.status === 'ACTIVE');
    if (activeVehicles.length === 0) {
      return;
    }
    const pick = activeVehicles[Math.floor(this.rng() * activeVehicles.length)]!;
    this.spikeVehicleState = { vehicleId: pick.vehicleId, remainingTicks: 5 };
  }

  private applyMovement(state: VehicleState): { speedKph: number; lat: number; lon: number; headingDeg: number } {
    const forceSpiked = this.shouldInject('spikes') && this.spikeVehicleState?.vehicleId === state.vehicleId && this.spikeVehicleState.remainingTicks > 0;
    const movement = moveVehicle(state, this.config.tickMs, this.rng, forceSpiked);
    if (forceSpiked && this.spikeVehicleState) {
      this.spikeVehicleState.remainingTicks -= 1;
      if (this.spikeVehicleState.remainingTicks <= 0) {
        this.spikeVehicleState = null;
      }
    }
    return movement;
  }

  tick(nowMs: number = Date.now()): SimulatorTickResult {
    const events: VehicleEvent[] = [];
    this.maybeStartSpikes();

    for (const vehicle of this.vehicles) {
      const before = cloneState(vehicle);
      applyTransition(vehicle, this.rng);

      const movement = this.applyMovement(vehicle);
      vehicle.headingDeg = movement.headingDeg;
      vehicle.lat = movement.lat;
      vehicle.lon = movement.lon;

      if (vehicle.status !== 'ACTIVE') {
        vehicle.speedKph = 0;
      } else {
        vehicle.speedKph = movement.speedKph;
      }

      if (this.shouldInject('teleport') && this.rng() < INJECT_TELEPORT_PCT) {
        vehicle.lat = randomUniform(this.rng, vehicle.region.latMin, vehicle.region.latMax);
        vehicle.lon = randomUniform(this.rng, vehicle.region.lonMin, vehicle.region.lonMax);
      }

      const eventBaseTime = nowMs;
      let occurredAt = eventBaseTime;
      if (this.shouldInject('late') && this.rng() < INJECT_LATE_PCT) {
        occurredAt = eventBaseTime - randomUniform(this.rng, MIN_LATE_MS, MAX_LATE_MS);
      }

      const event: VehicleEvent = {
        event_id: this.spawnEventId(),
        vehicle_id: vehicle.vehicleId,
        region: vehicle.region.code,
        lat: vehicle.lat,
        lon: vehicle.lon,
        speed_kph: vehicle.status === 'ACTIVE' ? clamp(vehicle.speedKph, 0, 500) : 0,
        heading_deg: vehicle.headingDeg,
        status: vehicle.status,
        occurred_at: Math.trunc(occurredAt),
      };

      events.push(event);
      if (this.shouldInject('dupes') && this.rng() < INJECT_DUPES_PCT) {
        events.push({ ...event });
      }

      if (before.status === 'ACTIVE' && vehicle.status === 'IDLE') {
        // this branch documents the required status-order requirement in tests.
      }
    }

    this.tickIndex += 1;
    return {
      events,
    };
  }

  getSpikesForTest(): { tickInterval: number; state: { vehicleId: string; remainingTicks: number } | null } {
    return {
      tickInterval: this.spikeInterval,
      state: this.spikeVehicleState ? { ...this.spikeVehicleState } : null,
    };
  }

  getConfig(): Readonly<SimulatorConfig> & { startedAtMs: number; regions: string } {
    return {
      ...this.config,
      startedAtMs: this.startedAtMs,
      regions: this.getRegions(),
    };
  }
}
