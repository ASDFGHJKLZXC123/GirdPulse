import { Kafka, logLevel, type Consumer, type EachBatchPayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { Pool } from 'pg';
import type { ProjectorConfig } from './config.js';
import { runMigrations } from './migrations.js';
import { loadProjectorOffsets, offsetKey } from './offsets.js';
import { processDecodedBatch } from './transactions.js';
import { applyRetention } from './retention.js';
import {
  PROJECTOR_PARTITION_COUNTS,
  PROJECTOR_TOPICS,
  coerceDecodedEvent,
  isProjectorTopic,
  type DecodedBatchMessage,
  type ProjectorTopic,
} from './types.js';
import {
  canonicalReplayPartitions,
  isWatermarkSatisfied,
  loadWatermark,
  nextReplayPartition,
  targetOffset,
  type ReplayTopicPartition,
  type WatermarkTargets,
} from './watermark.js';

const RETENTION_INTERVAL_MS = 60 * 60 * 1000;
const PROGRESS_INTERVAL = 10_000;

function normalizeRegistryUrl(raw: string): string {
  return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

async function loadEarliestOffsets(kafka: Kafka): Promise<Map<string, string>> {
  const admin = kafka.admin();
  const earliestOffsets = new Map<string, string>();
  await admin.connect();
  try {
    for (const topic of PROJECTOR_TOPICS) {
      const offsets = await admin.fetchTopicOffsets(topic);
      if (offsets.length !== PROJECTOR_PARTITION_COUNTS[topic]) {
        throw new Error(
          `${topic} has ${offsets.length} partitions; expected ${PROJECTOR_PARTITION_COUNTS[topic]}`,
        );
      }
      for (const offset of offsets) {
        if (
          !Number.isInteger(offset.partition) ||
          offset.partition < 0 ||
          offset.partition >= PROJECTOR_PARTITION_COUNTS[topic] ||
          !/^\d+$/.test(offset.low)
        ) {
          throw new Error(`Kafka returned an invalid earliest offset for ${topic}`);
        }
        earliestOffsets.set(offsetKey(topic, offset.partition), offset.low);
      }
    }
  } finally {
    await admin.disconnect();
  }
  return earliestOffsets;
}

function createTerminalSignal(): {
  promise: Promise<'signal' | 'watermark'>;
  resolve: (reason: 'signal' | 'watermark') => void;
  reject: (error: Error) => void;
} {
  let resolvePromise!: (reason: 'signal' | 'watermark') => void;
  let rejectPromise!: (error: Error) => void;
  const promise = new Promise<'signal' | 'watermark'>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return { promise, resolve: resolvePromise, reject: rejectPromise };
}

function seekAuthoritativeOffset(
  consumer: Consumer,
  topic: ProjectorTopic,
  partition: number,
  offsets: ReadonlyMap<string, string>,
  earliestOffsets: ReadonlyMap<string, string>,
): void {
  const key = offsetKey(topic, partition);
  const lastOffset = offsets.get(key);
  if (lastOffset !== undefined) {
    consumer.seek({
      topic,
      partition,
      offset: (BigInt(lastOffset) + 1n).toString(),
    });
    return;
  }

  const earliestOffset = earliestOffsets.get(key);
  if (earliestOffset === undefined) {
    throw new Error(`Missing earliest Kafka offset for ${topic}[${partition}]`);
  }
  consumer.seek({
    topic,
    partition,
    offset: earliestOffset,
  });
}

function seekStoredOffsets(
  consumer: Consumer,
  offsets: ReadonlyMap<string, string>,
  progressOffsets: ReadonlyMap<string, string>,
  earliestOffsets: ReadonlyMap<string, string>,
  assignment: Readonly<Record<string, number[]>>,
  targets: WatermarkTargets | undefined,
): void {
  for (const [topic, partitions] of Object.entries(assignment)) {
    if (!isProjectorTopic(topic)) {
      continue;
    }
    for (const partition of partitions) {
      const key = offsetKey(topic, partition);
      const progressOffset = progressOffsets.get(key);
      const target = targetOffset(targets, topic, partition);
      if (
        target !== undefined &&
        (BigInt(target) < 0n ||
          (progressOffset !== undefined && BigInt(progressOffset) >= BigInt(target)))
      ) {
        consumer.pause([{ topic, partitions: [partition] }]);
        continue;
      }
      seekAuthoritativeOffset(consumer, topic, partition, offsets, earliestOffsets);
    }
  }
}

function advanceOffset(offsets: Map<string, string>, key: string, nextOffset: string): void {
  if (!/^-?\d+$/.test(nextOffset)) {
    throw new Error(`Invalid Kafka offset ${nextOffset}`);
  }
  const currentOffset = offsets.get(key);
  if (currentOffset === undefined || BigInt(nextOffset) > BigInt(currentOffset)) {
    offsets.set(key, nextOffset);
  }
}

function boundedMessages(
  topic: ProjectorTopic,
  partition: number,
  messages: EachBatchPayload['batch']['messages'],
  targets: WatermarkTargets | undefined,
  persistedOffsets: ReadonlyMap<string, string>,
): EachBatchPayload['batch']['messages'] {
  const persisted = persistedOffsets.get(offsetKey(topic, partition));
  const target = targetOffset(targets, topic, partition);
  if (target !== undefined && BigInt(target) < 0n) {
    return [];
  }
  return messages.filter((message) => {
    const offset = BigInt(message.offset);
    return (
      (persisted === undefined || offset > BigInt(persisted)) &&
      (target === undefined || offset <= BigInt(target))
    );
  });
}

export async function runProjector(config: ProjectorConfig): Promise<void> {
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 8,
  });
  const terminal = createTerminalSignal();
  let retentionTimer: NodeJS.Timeout | undefined;
  let consumer: Consumer | undefined;

  try {
    await runMigrations(pool);
    console.log('projector migrations applied');

    if (config.retentionEnabled) {
      const deleted = await applyRetention(pool);
      console.log(`projector retention applied deleted=${deleted}`);
      retentionTimer = setInterval(() => {
        void applyRetention(pool)
          .then((count) => console.log(`projector retention applied deleted=${count}`))
          .catch((error: unknown) => console.error(`projector retention failed: ${String(error)}`));
      }, RETENTION_INTERVAL_MS);
      retentionTimer.unref();
    }

    const persistedOffsets = await loadProjectorOffsets(pool);
    const targets = config.watermarkFile ? await loadWatermark(config.watermarkFile) : undefined;
    const watermarkProgress = new Map(persistedOffsets);
    let replayPartitions: ReplayTopicPartition[] = [];
    let activeReplayPartition: ReplayTopicPartition | undefined;
    const registry = new SchemaRegistry({
      host: normalizeRegistryUrl(config.schemaRegistryUrl),
    });
    const kafka = new Kafka({
      clientId: 'gridpulse-projector',
      brokers: config.brokers,
      logLevel: logLevel.INFO,
    });
    const earliestOffsets = await loadEarliestOffsets(kafka);
    consumer = kafka.consumer({ groupId: config.groupId });
    let processedTotal = 0;
    let nextProgressLog = PROGRESS_INTERVAL;
    let completed = false;

    const completeWatermark = (): void => {
      if (!targets || completed || !isWatermarkSatisfied(targets, watermarkProgress)) {
        return;
      }
      completed = true;
      console.log('projector watermark reached');
      terminal.resolve('watermark');
    };

    const syncReplayPartition = (force = false): void => {
      if (!targets || !consumer) {
        return;
      }

      // A bounded replay needs a global order: duplicate event IDs can exist in different
      // partitions, and ON CONFLICT DO NOTHING must select the same first record every run.
      const next = nextReplayPartition(replayPartitions, targets, watermarkProgress);
      const activeKey = activeReplayPartition
        ? offsetKey(activeReplayPartition.topic, activeReplayPartition.partition)
        : undefined;
      const nextKey = next ? offsetKey(next.topic, next.partition) : undefined;

      if (force || activeKey !== nextKey) {
        for (const topicPartition of replayPartitions) {
          const selection = [
            {
              topic: topicPartition.topic,
              partitions: [topicPartition.partition],
            },
          ];
          consumer.pause(selection);
        }
        if (next) {
          seekAuthoritativeOffset(
            consumer,
            next.topic,
            next.partition,
            persistedOffsets,
            earliestOffsets,
          );
          consumer.resume([{ topic: next.topic, partitions: [next.partition] }]);
        }
        activeReplayPartition = next;
        if (next) {
          console.log(
            `projector replay partition active topic=${next.topic} partition=${next.partition}`,
          );
        }
      }

      completeWatermark();
    };

    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      seekStoredOffsets(
        consumer!,
        persistedOffsets,
        watermarkProgress,
        earliestOffsets,
        event.payload.memberAssignment,
        targets,
      );
      replayPartitions = targets ? canonicalReplayPartitions(event.payload.memberAssignment) : [];
      activeReplayPartition = undefined;
      console.log('projector consumer joined');
      if (targets) {
        syncReplayPartition(true);
      } else {
        completeWatermark();
      }
    });
    consumer.on(consumer.events.END_BATCH_PROCESS, (event) => {
      if (event.payload.batchSize !== 0 || !isProjectorTopic(event.payload.topic)) {
        return;
      }

      const topic = event.payload.topic;
      if (
        targets &&
        (!activeReplayPartition ||
          activeReplayPartition.topic !== topic ||
          activeReplayPartition.partition !== event.payload.partition)
      ) {
        return;
      }
      const key = offsetKey(topic, event.payload.partition);
      advanceOffset(watermarkProgress, key, event.payload.lastOffset);

      const target = targetOffset(targets, topic, event.payload.partition);
      if (target !== undefined && BigInt(event.payload.lastOffset) >= BigInt(target)) {
        consumer!.pause([{ topic, partitions: [event.payload.partition] }]);
      }
      if (targets) {
        syncReplayPartition();
      } else {
        completeWatermark();
      }
    });
    consumer.on(consumer.events.CRASH, (event) => {
      terminal.reject(event.payload.error);
    });

    const signalShutdown = (): void => terminal.resolve('signal');
    process.once('SIGINT', signalShutdown);
    process.once('SIGTERM', signalShutdown);

    await consumer.connect();
    await consumer.subscribe({
      topics: [...PROJECTOR_TOPICS],
      fromBeginning: true,
    });

    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 1,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        if (!isRunning() || isStale()) {
          return;
        }
        if (!isProjectorTopic(batch.topic)) {
          throw new Error(`Unexpected topic ${batch.topic}`);
        }

        const topic = batch.topic;
        if (
          targets &&
          (!activeReplayPartition ||
            activeReplayPartition.topic !== topic ||
            activeReplayPartition.partition !== batch.partition)
        ) {
          return;
        }
        await heartbeat();
        const messages = boundedMessages(
          topic,
          batch.partition,
          batch.messages,
          targets,
          persistedOffsets,
        );
        const decoded: DecodedBatchMessage[] = [];
        for (const message of messages) {
          if (message.value === null) {
            throw new Error(`Null value at ${topic}[${batch.partition}] offset ${message.offset}`);
          }
          const value = coerceDecodedEvent(topic, await registry.decode(message.value));
          decoded.push({ offset: message.offset, value });
          if (decoded.length % 100 === 0) {
            await heartbeat();
          }
        }

        await heartbeat();
        const result = await processDecodedBatch(
          pool,
          {
            topic,
            partition: batch.partition,
            messages: decoded,
          },
          {
            swapCoords: config.bugSwapCoords,
            heartbeat,
          },
        );

        if (result.lastOffset !== null) {
          const key = offsetKey(topic, batch.partition);
          advanceOffset(persistedOffsets, key, result.lastOffset);
          advanceOffset(watermarkProgress, key, result.lastOffset);
          resolveOffset(result.lastOffset);
          processedTotal += result.processed;
          while (processedTotal >= nextProgressLog) {
            console.log(`projector progress messages=${nextProgressLog}`);
            nextProgressLog += PROGRESS_INTERVAL;
          }
        }

        const key = offsetKey(topic, batch.partition);
        advanceOffset(watermarkProgress, key, batch.lastOffset());
        const target = targetOffset(targets, topic, batch.partition);
        if (target !== undefined && BigInt(batch.lastOffset()) >= BigInt(target)) {
          consumer!.pause([{ topic, partitions: [batch.partition] }]);
        }

        await heartbeat();
        if (targets) {
          syncReplayPartition();
        } else {
          completeWatermark();
        }
      },
    });

    const reason = await terminal.promise;
    if (reason === 'watermark') {
      console.log('projector replay complete');
    } else {
      console.log('projector shutdown requested');
    }
  } finally {
    if (retentionTimer) {
      clearInterval(retentionTimer);
    }
    if (consumer) {
      await consumer.stop().catch(() => undefined);
      await consumer.disconnect().catch(() => undefined);
    }
    await pool.end();
  }
}
