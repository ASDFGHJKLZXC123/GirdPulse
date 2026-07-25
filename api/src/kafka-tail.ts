import { randomUUID } from 'node:crypto';

import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { Kafka, type Consumer } from 'kafkajs';

import type { ApiConfig } from './config.js';
import { ANOMALY_DETECTED_TOPIC, type EventBus, VEHICLE_MOVED_TOPIC } from './event-bus.js';
import { mapAnomaly, mapVehicleEvent } from './event-mappers.js';

export const VEHICLE_EVENTS_TOPIC = 'fleet.vehicle-events';
export const ANOMALIES_TOPIC = 'fleet.anomalies';

const GROUP_ID_PREFIX = 'gridpulse-api-subs-';

export interface KafkaTailOptions {
  groupId?: string;
}

function normalizeRegistryUrl(raw: string): string {
  return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class KafkaTail {
  readonly groupId: string;

  private readonly consumer: Consumer;
  private readonly registry: SchemaRegistry;
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private rejectGroupJoin: ((error: Error) => void) | undefined;
  private removeCrashListener: (() => void) | undefined;
  private connected = false;
  private runStarted = false;
  private stopped = false;

  constructor(
    config: Pick<ApiConfig, 'brokers' | 'schemaRegistryUrl'>,
    private readonly eventBus: EventBus,
    options: KafkaTailOptions = {},
  ) {
    this.groupId = options.groupId ?? `${GROUP_ID_PREFIX}${randomUUID()}`;
    this.registry = new SchemaRegistry({
      host: normalizeRegistryUrl(config.schemaRegistryUrl),
    });
    this.consumer = new Kafka({
      clientId: 'gridpulse-api',
      brokers: [...config.brokers],
    }).consumer({ groupId: this.groupId });
  }

  start(): Promise<void> {
    if (this.stopped) {
      return Promise.reject(new Error('Kafka tail cannot be restarted after it has stopped'));
    }
    this.startPromise ??= this.startConsumer();
    return this.startPromise;
  }

  stop(): Promise<void> {
    this.stopped = true;
    this.rejectGroupJoin?.(new Error('Kafka tail stopped before joining its consumer group'));
    this.stopPromise ??= this.stopConsumer();
    return this.stopPromise;
  }

  private async startConsumer(): Promise<void> {
    let groupReady = false;
    let resolveGroupJoin!: () => void;
    let rejectGroupJoin!: (error: Error) => void;
    const groupJoined = new Promise<void>((resolve, reject) => {
      resolveGroupJoin = resolve;
      rejectGroupJoin = reject;
    });
    this.rejectGroupJoin = rejectGroupJoin;

    const removeGroupJoinListener = this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      groupReady = true;
      resolveGroupJoin();
    });
    this.removeCrashListener = this.consumer.on(this.consumer.events.CRASH, (event) => {
      if (!groupReady) {
        rejectGroupJoin(event.payload.error);
        return;
      }
      console.error(`api Kafka subscription tail crashed: ${errorMessage(event.payload.error)}`);
    });

    try {
      await this.consumer.connect();
      this.connected = true;
      this.ensureRunning();

      await this.consumer.subscribe({
        topics: [VEHICLE_EVENTS_TOPIC, ANOMALIES_TOPIC],
        fromBeginning: false,
      });
      this.ensureRunning();

      await this.consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          if (message.value === null) {
            throw new Error(`Null value at ${topic}[${partition}] offset ${message.offset}`);
          }

          const decoded: unknown = await this.registry.decode(message.value);
          if (topic === VEHICLE_EVENTS_TOPIC) {
            this.eventBus.publish(VEHICLE_MOVED_TOPIC, mapVehicleEvent(decoded));
            return;
          }
          if (topic === ANOMALIES_TOPIC) {
            this.eventBus.publish(ANOMALY_DETECTED_TOPIC, mapAnomaly(decoded));
            return;
          }
          throw new Error(`Unexpected subscription topic ${topic}`);
        },
      });
      this.runStarted = true;
      this.ensureRunning();
      await groupJoined;
    } catch (error) {
      this.stopped = true;
      await this.disconnectConsumer();
      throw error;
    } finally {
      removeGroupJoinListener();
      this.rejectGroupJoin = undefined;
    }
  }

  private async stopConsumer(): Promise<void> {
    if (this.startPromise) {
      await this.startPromise.catch(() => undefined);
    }
    await this.disconnectConsumer();
  }

  private async disconnectConsumer(): Promise<void> {
    this.removeCrashListener?.();
    this.removeCrashListener = undefined;

    if (this.runStarted) {
      await this.consumer.stop().catch(() => undefined);
      this.runStarted = false;
    }
    if (this.connected) {
      await this.consumer.disconnect().catch(() => undefined);
      this.connected = false;
    }
  }

  private ensureRunning(): void {
    if (this.stopped) {
      throw new Error('Kafka tail stopped during startup');
    }
  }
}
