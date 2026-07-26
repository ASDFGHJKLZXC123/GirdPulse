import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';

import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { createClient, type Client } from 'graphql-ws';
import { Kafka, logLevel, type Admin, type Producer } from 'kafkajs';
import { Pool } from 'pg';
import WebSocket from 'ws';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { ApiConfig } from '../src/config.js';
import { ANOMALY_DETECTED_TOPIC, EventBus, type EventPayloads } from '../src/event-bus.js';
import { ANOMALIES_TOPIC, VEHICLE_EVENTS_TOPIC } from '../src/kafka-tail.js';
import { startApi, type RunningApi } from '../src/server.js';

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgresql://gridpulse:gridpulse@127.0.0.1:5432/gridpulse';
const BROKERS = (process.env.BROKERS ?? '127.0.0.1:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? 'http://127.0.0.1:8081';
const VEHICLE_SUBJECT = 'fleet.vehicle-events-value';
const ANOMALY_SUBJECT = 'fleet.anomalies-value';
const TEST_RUN = randomUUID().replaceAll('-', '').slice(0, 12);
const TARGET_REGION = `M06_${TEST_RUN}`;
const OTHER_REGION = `M06_OTHER_${TEST_RUN}`;

interface SubscriptionResult {
  data?: Record<string, unknown>;
  errors?: ReadonlyArray<{ message: string }>;
}

interface LiveSubscription {
  client: Client;
  first: Promise<IteratorResult<SubscriptionResult>>;
  iterator: AsyncIterableIterator<SubscriptionResult>;
}

let admin: Admin;
let producer: Producer;
let registry: SchemaRegistry;
let pool: Pool;
let api: RunningApi;
let vehicleSchemaId: number;
let anomalySchemaId: number;
const openClients = new Set<Client>();

function config(): ApiConfig {
  return {
    port: 0,
    databaseUrl: DATABASE_URL,
    brokers: BROKERS,
    schemaRegistryUrl: SCHEMA_REGISTRY_URL,
  };
}

function websocketUrl(runningApi: RunningApi): string {
  return runningApi.url.replace(/^http:/, 'ws:').replace(/^https:/, 'wss:');
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms`)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

async function openSubscription(runningApi: RunningApi, query: string): Promise<LiveSubscription> {
  let resolveConnected!: () => void;
  let rejectConnected!: (error: unknown) => void;
  const connected = new Promise<void>((resolve, reject) => {
    resolveConnected = resolve;
    rejectConnected = reject;
  });
  const client = createClient({
    url: websocketUrl(runningApi),
    webSocketImpl: WebSocket,
    lazy: false,
    retryAttempts: 0,
    on: {
      connected: resolveConnected,
      error: rejectConnected,
    },
  });
  openClients.add(client);
  const iterator = client.iterate({ query }) as AsyncIterableIterator<SubscriptionResult>;
  const first = iterator.next();

  await withTimeout(connected, 2_000);
  await delay(75);
  return { client, first, iterator };
}

async function closeSubscription(subscription: LiveSubscription): Promise<void> {
  await subscription.iterator.return?.();
  await subscription.client.dispose();
  openClients.delete(subscription.client);
}

async function produceVehicleEvent(region: string, eventId = randomUUID()): Promise<string> {
  const value = await registry.encode(vehicleSchemaId, {
    event_id: eventId,
    vehicle_id: `veh-${TEST_RUN}`,
    region,
    lat: 47.6062,
    lon: -122.3321,
    speed_kph: 42.5,
    heading_deg: 90,
    status: 'ACTIVE',
    occurred_at: Date.now(),
  });
  await producer.send({
    topic: VEHICLE_EVENTS_TOPIC,
    messages: [{ key: `veh-${TEST_RUN}`, value }],
  });
  return eventId;
}

async function produceAnomaly(region: string, anomalyId = randomUUID()): Promise<string> {
  const windowEnd = Date.now();
  const value = await registry.encode(anomalySchemaId, {
    anomaly_id: anomalyId,
    vehicle_id: `veh-${TEST_RUN}`,
    region,
    kind: 'SPEED_THRESHOLD',
    value: 91.25,
    detector_version: 1,
    window_start: windowEnd - 60_000,
    window_end: windowEnd,
  });
  await producer.send({
    topic: ANOMALIES_TOPIC,
    messages: [{ key: `veh-${TEST_RUN}`, value }],
  });
  return anomalyId;
}

async function expectNoCommittedOffsets(groupId: string): Promise<void> {
  const offsets = await admin.fetchOffsets({
    groupId,
    topics: [VEHICLE_EVENTS_TOPIC, ANOMALIES_TOPIC],
    resolveOffsets: false,
  });
  const partitions = offsets.flatMap((topic) => topic.partitions);

  expect(partitions.length).toBeGreaterThan(0);
  expect(partitions.every((partition) => partition.offset === '-1')).toBe(true);
}

async function findAnomaly(
  iterator: AsyncIterableIterator<EventPayloads['ANOMALY_DETECTED']>,
  first: Promise<IteratorResult<EventPayloads['ANOMALY_DETECTED']>>,
  anomalyId: string,
  timeoutMs: number,
): Promise<EventPayloads['ANOMALY_DETECTED'] | undefined> {
  const deadline = performance.now() + timeoutMs;
  let next = first;

  while (performance.now() < deadline) {
    const remaining = Math.max(1, deadline - performance.now());
    const observed = await Promise.race([
      next.then((result) => ({ kind: 'result' as const, result })),
      delay(remaining).then(() => ({ kind: 'timeout' as const })),
    ]);
    if (observed.kind === 'timeout' || observed.result.done) {
      return undefined;
    }
    if (observed.result.value.anomalyDetected.id === anomalyId) {
      return observed.result.value;
    }
    next = iterator.next();
  }
  return undefined;
}

beforeAll(async () => {
  const kafka = new Kafka({
    clientId: `gridpulse-m06-test-${TEST_RUN}`,
    brokers: BROKERS,
    logLevel: logLevel.ERROR,
  });
  admin = kafka.admin();
  producer = kafka.producer();
  registry = new SchemaRegistry({
    host: SCHEMA_REGISTRY_URL.replace(/\/$/, ''),
  });
  pool = new Pool({
    connectionString: DATABASE_URL,
    application_name: `gridpulse-m06-kafka-test-${TEST_RUN}`,
    max: 2,
  });

  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      { topic: VEHICLE_EVENTS_TOPIC, numPartitions: 6, replicationFactor: 1 },
      { topic: ANOMALIES_TOPIC, numPartitions: 6, replicationFactor: 1 },
    ],
  });
  await producer.connect();
  [vehicleSchemaId, anomalySchemaId] = await Promise.all([
    registry.getLatestSchemaId(VEHICLE_SUBJECT),
    registry.getLatestSchemaId(ANOMALY_SUBJECT),
  ]);
  api = await startApi({ config: config(), pool });
}, 30_000);

afterAll(async () => {
  for (const client of openClients) {
    try {
      await client.dispose();
    } catch {
      // Continue closing the remaining shared test resources.
    }
  }
  await api?.stop();
  await producer?.disconnect();
  await admin?.disconnect();
  await pool?.end();
});

describe.sequential('M06 Redpanda to graphql-ws subscriptions', () => {
  it('delivers only the requested region from a two-region vehicle stream', async () => {
    const subscription = await openSubscription(
      api,
      `
        subscription TestVehicleTail {
          vehicleMoved(region: "${TARGET_REGION}") {
            eventId
            vehicleId
            occurredAt
          }
        }
      `,
    );
    const targetEventId = randomUUID();

    await produceVehicleEvent(OTHER_REGION);
    await produceVehicleEvent(TARGET_REGION, targetEventId);

    const result = await withTimeout(subscription.first, 2_000);
    expect(result.value.errors).toBeUndefined();
    expect(result.value.data).toMatchObject({
      vehicleMoved: {
        eventId: targetEventId,
        vehicleId: `veh-${TEST_RUN}`,
      },
    });
    await closeSubscription(subscription);
  }, 10_000);

  it('delivers an anomaly within two seconds of the Kafka produce acknowledgement', async () => {
    const subscription = await openSubscription(
      api,
      `
        subscription TestAnomalyLatency {
          anomalyDetected(region: "${TARGET_REGION}") {
            id
            region
            kind
            detectorVersion
            windowStart
            windowEnd
          }
        }
      `,
    );
    const anomalyId = randomUUID();

    await produceAnomaly(TARGET_REGION, anomalyId);
    const landedAt = performance.now();
    const result = await withTimeout(subscription.first, 2_000);
    const latencyMs = performance.now() - landedAt;

    expect(result.value.errors).toBeUndefined();
    expect(result.value.data).toMatchObject({
      anomalyDetected: {
        id: anomalyId,
        region: TARGET_REGION,
        kind: 'SPEED_THRESHOLD',
        detectorVersion: 1,
      },
    });
    expect(latencyMs).toBeLessThanOrEqual(2_000);
    await expectNoCommittedOffsets(api.groupId);
    await closeSubscription(subscription);
  }, 10_000);

  it('uses a fresh group after restart and does not replay a missed record', async () => {
    const firstGroupId = api.groupId;
    await api.stop();

    const missedId = await produceAnomaly(TARGET_REGION);
    const restartBus = new EventBus();
    const missedObservation = restartBus.subscribe(ANOMALY_DETECTED_TOPIC);
    const possibleMissed = missedObservation.next();
    api = await startApi({
      config: config(),
      pool,
      eventBus: restartBus,
    });
    expect(api.groupId).toMatch(/^gridpulse-api-subs-.+/);
    expect(api.groupId).not.toBe(firstGroupId);

    const replayedMissed = await findAnomaly(missedObservation, possibleMissed, missedId, 500);
    expect(replayedMissed).toBeUndefined();
    await missedObservation.return?.();

    const subscription = await openSubscription(
      api,
      `
        subscription TestRestartTail {
          anomalyDetected(region: "${TARGET_REGION}") {
            id
          }
        }
      `,
    );
    const liveId = await produceAnomaly(TARGET_REGION);
    const result = await withTimeout(subscription.first, 2_000);

    expect(result.value.errors).toBeUndefined();
    expect(result.value.data).toEqual({
      anomalyDetected: {
        id: liveId,
      },
    });
    expect(result.value.data).not.toEqual({
      anomalyDetected: {
        id: missedId,
      },
    });
    await expectNoCommittedOffsets(api.groupId);
    await closeSubscription(subscription);
  }, 15_000);
});
