import { parse, subscribe, type ExecutionResult } from 'graphql';
import { describe, expect, it, vi } from 'vitest';

import { ANOMALY_DETECTED_TOPIC, EventBus, VEHICLE_MOVED_TOPIC } from '../src/event-bus.js';
import {
  mapAnomaly,
  mapVehicleEvent,
  type AnomalyDetectedPayload,
  type VehicleMovedPayload,
} from '../src/event-mappers.js';
import type { Database } from '../src/model.js';
import { createRequestContext } from '../src/resolvers.js';
import { createApiSchema } from '../src/schema.js';

const unusedDatabase = {
  query: vi.fn(async () => {
    throw new Error('Subscription resolvers must not query PostgreSQL');
  }),
} as unknown as Database;

function withTimeout<T>(promise: Promise<T>, timeoutMs = 1_000): Promise<T> {
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

async function openSubscription(
  eventBus: EventBus,
  source: string,
): Promise<AsyncIterableIterator<ExecutionResult>> {
  const result = await subscribe({
    schema: createApiSchema(),
    document: parse(source),
    contextValue: createRequestContext(unusedDatabase, eventBus),
  });
  if (!(Symbol.asyncIterator in result)) {
    throw new Error(`Subscription failed: ${JSON.stringify(result.errors ?? result)}`);
  }
  return result as AsyncIterableIterator<ExecutionResult>;
}

function vehiclePayload(region: string, eventId: string): VehicleMovedPayload {
  return {
    region,
    vehicleMoved: {
      eventId,
      vehicleId: `veh-${region.toLowerCase()}`,
      lat: region === 'SEA' ? 47.6062 : 37.7749,
      lon: region === 'SEA' ? -122.3321 : -122.4194,
      speedKph: 42.5,
      occurredAt: new Date('2026-07-24T12:00:00.000Z'),
    },
  };
}

function anomalyPayload(region: string): AnomalyDetectedPayload {
  return {
    region,
    anomalyDetected: {
      id: '20000000-0000-4000-8000-000000000099',
      vehicleId: `veh-${region.toLowerCase()}`,
      region,
      kind: 'SPEED_THRESHOLD',
      value: 91.25,
      detectorVersion: 1,
      windowStart: new Date('2026-07-24T12:00:00.000Z'),
      windowEnd: new Date('2026-07-24T12:01:00.000Z'),
    },
  };
}

describe('M06 in-memory subscription bus', () => {
  it('filters a two-region vehicle stream in the subscribe resolver', async () => {
    const eventBus = new EventBus();
    const iterator = await openSubscription(
      eventBus,
      `
        subscription SeaVehicles {
          vehicleMoved(region: "SEA") {
            eventId
            vehicleId
            lat
            lon
            speedKph
            occurredAt
          }
        }
      `,
    );

    const next = iterator.next();
    eventBus.publish(
      VEHICLE_MOVED_TOPIC,
      vehiclePayload('SFO', '10000000-0000-4000-8000-000000000098'),
    );
    eventBus.publish(
      VEHICLE_MOVED_TOPIC,
      vehiclePayload('SEA', '10000000-0000-4000-8000-000000000099'),
    );

    await expect(withTimeout(next)).resolves.toEqual({
      value: {
        data: {
          vehicleMoved: {
            eventId: '10000000-0000-4000-8000-000000000099',
            vehicleId: 'veh-sea',
            lat: 47.6062,
            lon: -122.3321,
            speedKph: 42.5,
            occurredAt: new Date('2026-07-24T12:00:00.000Z'),
          },
        },
      },
      done: false,
    });
    expect(unusedDatabase.query).not.toHaveBeenCalled();

    await iterator.return?.();
  });

  it('filters anomalyDetected by region and keeps each topic isolated', async () => {
    const eventBus = new EventBus();
    const iterator = await openSubscription(
      eventBus,
      `
        subscription SeaAnomalies {
          anomalyDetected(region: "SEA") {
            id
            vehicleId
            region
            kind
            value
            detectorVersion
            windowStart
            windowEnd
          }
        }
      `,
    );

    const next = iterator.next();
    eventBus.publish(
      VEHICLE_MOVED_TOPIC,
      vehiclePayload('SEA', '10000000-0000-4000-8000-000000000097'),
    );
    eventBus.publish(ANOMALY_DETECTED_TOPIC, anomalyPayload('SFO'));
    eventBus.publish(ANOMALY_DETECTED_TOPIC, anomalyPayload('SEA'));

    const result = await withTimeout(next);
    expect(result.value).toEqual({
      data: {
        anomalyDetected: {
          id: '20000000-0000-4000-8000-000000000099',
          vehicleId: 'veh-sea',
          region: 'SEA',
          kind: 'SPEED_THRESHOLD',
          value: 91.25,
          detectorVersion: 1,
          windowStart: new Date('2026-07-24T12:00:00.000Z'),
          windowEnd: new Date('2026-07-24T12:01:00.000Z'),
        },
      },
    });
    expect(unusedDatabase.query).not.toHaveBeenCalled();

    await iterator.return?.();
  });

  it('decodes Avro-shaped records into camelCase GraphQL payloads', () => {
    expect(
      mapVehicleEvent({
        event_id: '10000000-0000-4000-8000-000000000096',
        vehicle_id: 'veh-sea',
        region: 'SEA',
        lat: 47.6062,
        lon: -122.3321,
        speed_kph: 42.5,
        occurred_at: Date.parse('2026-07-24T12:00:00.000Z'),
      }),
    ).toEqual({
      region: 'SEA',
      vehicleMoved: {
        eventId: '10000000-0000-4000-8000-000000000096',
        vehicleId: 'veh-sea',
        lat: 47.6062,
        lon: -122.3321,
        speedKph: 42.5,
        occurredAt: new Date('2026-07-24T12:00:00.000Z'),
      },
    });

    expect(
      mapAnomaly({
        anomaly_id: '20000000-0000-4000-8000-000000000096',
        vehicle_id: 'veh-sea',
        region: 'SEA',
        kind: 'SPEED_ZSCORE',
        value: 4.75,
        detector_version: 2,
        window_start: Date.parse('2026-07-24T12:00:00.000Z'),
        window_end: Date.parse('2026-07-24T12:01:00.000Z'),
      }),
    ).toEqual({
      region: 'SEA',
      anomalyDetected: {
        id: '20000000-0000-4000-8000-000000000096',
        vehicleId: 'veh-sea',
        region: 'SEA',
        kind: 'SPEED_ZSCORE',
        value: 4.75,
        detectorVersion: 2,
        windowStart: new Date('2026-07-24T12:00:00.000Z'),
        windowEnd: new Date('2026-07-24T12:01:00.000Z'),
      },
    });
  });
});
