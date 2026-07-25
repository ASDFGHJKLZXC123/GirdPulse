import { readFile } from 'node:fs/promises';
import { PROJECTOR_PARTITION_COUNTS, PROJECTOR_TOPICS, type ProjectorTopic } from './types.js';
import { offsetKey } from './offsets.js';

export interface WatermarkFile {
  version: 1;
  captured_at: string;
  topics: Record<ProjectorTopic, Record<string, string>>;
  low_offsets: Record<ProjectorTopic, Record<string, string>>;
}

export type WatermarkTargets = Map<string, string>;

export interface ReplayTopicPartition {
  topic: ProjectorTopic;
  partition: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function validatePartitionMap(
  topic: ProjectorTopic,
  value: unknown,
  fieldName: 'topics' | 'low_offsets',
): Record<string, string> {
  if (!isRecord(value)) {
    throw new Error(`Watermark is missing ${fieldName}.${topic}`);
  }

  const expectedCount = PROJECTOR_PARTITION_COUNTS[topic];
  const expectedKeys = Array.from({ length: expectedCount }, (_, partition) => String(partition));
  const actualKeys = Object.keys(value).sort((left, right) => Number(left) - Number(right));
  if (
    actualKeys.length !== expectedKeys.length ||
    actualKeys.some((key, index) => key !== expectedKeys[index])
  ) {
    throw new Error(
      `Watermark ${fieldName}.${topic} must contain exactly partitions ${expectedKeys.join(',')}`,
    );
  }

  const validated: Record<string, string> = {};
  for (const key of expectedKeys) {
    const offset = value[key];
    if (typeof offset !== 'string' || !/^-?\d+$/.test(offset)) {
      throw new Error(`Invalid watermark offset ${fieldName}.${topic}[${key}]`);
    }
    if (fieldName === 'topics' && BigInt(offset) < -1n) {
      throw new Error(`Watermark target ${topic}[${key}] must be at least -1`);
    }
    if (fieldName === 'low_offsets' && BigInt(offset) < 0n) {
      throw new Error(`Watermark low offset ${topic}[${key}] must be non-negative`);
    }
    validated[key] = offset;
  }
  return validated;
}

export async function loadWatermarkFile(path: string): Promise<WatermarkFile> {
  const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
  if (
    !isRecord(parsed) ||
    parsed.version !== 1 ||
    typeof parsed.captured_at !== 'string' ||
    !Number.isFinite(Date.parse(parsed.captured_at)) ||
    !isRecord(parsed.topics) ||
    !isRecord(parsed.low_offsets)
  ) {
    throw new Error(`Invalid watermark file: ${path}`);
  }

  const topics = {} as WatermarkFile['topics'];
  const lowOffsets = {} as WatermarkFile['low_offsets'];
  for (const topic of PROJECTOR_TOPICS) {
    topics[topic] = validatePartitionMap(topic, parsed.topics[topic], 'topics');
    lowOffsets[topic] = validatePartitionMap(topic, parsed.low_offsets[topic], 'low_offsets');
    for (let partition = 0; partition < PROJECTOR_PARTITION_COUNTS[topic]; partition += 1) {
      const key = String(partition);
      const target = topics[topic][key];
      const lowOffset = lowOffsets[topic][key];
      if (target !== '-1' && BigInt(target) < BigInt(lowOffset)) {
        throw new Error(
          `Watermark target ${topic}[${partition}]=${target} is below captured low offset ${lowOffset}`,
        );
      }
    }
  }

  return {
    version: 1,
    captured_at: parsed.captured_at,
    topics,
    low_offsets: lowOffsets,
  };
}

export async function loadWatermark(path: string): Promise<WatermarkTargets> {
  const watermark = await loadWatermarkFile(path);
  const targets = new Map<string, string>();
  for (const topic of PROJECTOR_TOPICS) {
    for (const [partitionText, offset] of Object.entries(watermark.topics[topic])) {
      targets.set(offsetKey(topic, Number(partitionText)), offset);
    }
  }
  return targets;
}

export function targetOffset(
  targets: WatermarkTargets | undefined,
  topic: ProjectorTopic,
  partition: number,
): string | undefined {
  return targets?.get(offsetKey(topic, partition));
}

export function canonicalReplayPartitions(
  assignment: Readonly<Record<string, readonly number[]>>,
): ReplayTopicPartition[] {
  const ordered: ReplayTopicPartition[] = [];

  for (const topic of PROJECTOR_TOPICS) {
    const partitions = [...(assignment[topic] ?? [])].sort((left, right) => left - right);
    const expected = Array.from(
      { length: PROJECTOR_PARTITION_COUNTS[topic] },
      (_, partition) => partition,
    );
    if (
      partitions.length !== expected.length ||
      partitions.some((partition, index) => partition !== expected[index])
    ) {
      throw new Error(
        `Fixed-watermark replay requires exclusive assignment of ${topic}[${expected.join(',')}]; got [${partitions.join(',')}]`,
      );
    }
    for (const partition of partitions) {
      ordered.push({ topic, partition });
    }
  }

  return ordered;
}

export function nextReplayPartition(
  partitions: readonly ReplayTopicPartition[],
  targets: WatermarkTargets,
  progress: ReadonlyMap<string, string>,
): ReplayTopicPartition | undefined {
  return partitions.find(({ topic, partition }) => {
    const target = targetOffset(targets, topic, partition);
    if (target === undefined || BigInt(target) < 0n) {
      return false;
    }
    const current = progress.get(offsetKey(topic, partition));
    return current === undefined || BigInt(current) < BigInt(target);
  });
}

export function isWatermarkSatisfied(
  targets: WatermarkTargets,
  persistedOffsets: ReadonlyMap<string, string>,
): boolean {
  for (const [key, target] of targets) {
    if (BigInt(target) < 0n) {
      continue;
    }
    const persisted = persistedOffsets.get(key);
    if (persisted === undefined || BigInt(persisted) < BigInt(target)) {
      return false;
    }
  }
  return true;
}
