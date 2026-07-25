import { mkdir, rename, unlink, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { Kafka, logLevel } from 'kafkajs';
import { loadConfig } from '../config.js';
import { PROJECTOR_PARTITION_COUNTS, PROJECTOR_TOPICS, type ProjectorTopic } from '../types.js';
import type { WatermarkFile } from '../watermark.js';

interface Arguments {
  output: string;
  stableMs: number;
}

interface CapturedOffsets {
  topics: Record<ProjectorTopic, Record<string, string>>;
  lowOffsets: Record<ProjectorTopic, Record<string, string>>;
}

function usage(): never {
  throw new Error('usage: watermark.ts --output <file> [--stable-ms <milliseconds>]');
}

function parseArguments(argv: string[]): Arguments {
  let output: string | undefined;
  let stableMs = 1_000;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--output') {
      output = argv[index + 1];
      index += 1;
    } else if (argument === '--stable-ms') {
      const raw = argv[index + 1];
      stableMs = Number(raw);
      index += 1;
    } else {
      usage();
    }
  }

  if (!output || !Number.isInteger(stableMs) || stableMs < 0 || stableMs > 60_000) {
    usage();
  }

  return { output: resolve(output), stableMs };
}

function terminalOffset(lowWatermark: string, highWatermark: string): string {
  if (!/^\d+$/.test(lowWatermark) || !/^\d+$/.test(highWatermark)) {
    throw new Error(`Kafka returned invalid low/high watermarks: ${lowWatermark}/${highWatermark}`);
  }
  if (BigInt(highWatermark) < BigInt(lowWatermark)) {
    throw new Error(
      `Kafka returned high watermark ${highWatermark} below low watermark ${lowWatermark}`,
    );
  }
  if (BigInt(highWatermark) === BigInt(lowWatermark)) {
    return '-1';
  }
  return (BigInt(highWatermark) - 1n).toString();
}

async function fetchTerminalOffsets(
  admin: ReturnType<ReturnType<typeof createKafka>['admin']>,
): Promise<CapturedOffsets> {
  const topics = {} as CapturedOffsets['topics'];
  const lowOffsets = {} as CapturedOffsets['lowOffsets'];

  for (const topic of PROJECTOR_TOPICS) {
    const offsets = await admin.fetchTopicOffsets(topic);
    const partitions: Record<string, string> = {};
    const lows: Record<string, string> = {};
    for (const offset of offsets) {
      if (!Number.isInteger(offset.partition) || offset.partition < 0) {
        throw new Error(`Kafka returned an invalid partition for ${topic}`);
      }
      partitions[String(offset.partition)] = terminalOffset(offset.low, offset.high);
      lows[String(offset.partition)] = offset.low;
    }

    const expectedKeys = Array.from({ length: PROJECTOR_PARTITION_COUNTS[topic] }, (_, partition) =>
      String(partition),
    );
    const actualKeys = Object.keys(partitions).sort((left, right) => Number(left) - Number(right));
    if (
      actualKeys.length !== expectedKeys.length ||
      actualKeys.some((key, index) => key !== expectedKeys[index])
    ) {
      throw new Error(
        `${topic} must contain exactly partitions ${expectedKeys.join(',')}; got ${actualKeys.join(',')}`,
      );
    }
    topics[topic] = partitions;
    lowOffsets[topic] = lows;
  }

  return { topics, lowOffsets };
}

function createKafka(brokers: string[]): Kafka {
  return new Kafka({
    clientId: 'gridpulse-watermark',
    brokers,
    logLevel: logLevel.NOTHING,
  });
}

function offsetsEqual(left: CapturedOffsets, right: CapturedOffsets): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

async function writeJsonAtomically(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.${process.pid}.tmp`;
  try {
    await writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, {
      encoding: 'utf8',
      mode: 0o600,
    });
    await rename(temporaryPath, path);
  } finally {
    await unlink(temporaryPath).catch(() => undefined);
  }
}

async function main(): Promise<void> {
  const args = parseArguments(process.argv.slice(2));
  const config = loadConfig();
  const admin = createKafka(config.brokers).admin();

  await admin.connect();
  try {
    const first = await fetchTerminalOffsets(admin);
    if (args.stableMs > 0) {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, args.stableMs));
      const second = await fetchTerminalOffsets(admin);
      if (!offsetsEqual(first, second)) {
        throw new Error(
          'Kafka offsets changed after producers were stopped; refusing to record a moving watermark',
        );
      }
    }

    const watermark: WatermarkFile = {
      version: 1,
      captured_at: new Date().toISOString(),
      topics: first.topics,
      low_offsets: first.lowOffsets,
    };
    await writeJsonAtomically(args.output, watermark);

    const partitionCount = Object.values(first.topics).reduce(
      (count, partitions) => count + Object.keys(partitions).length,
      0,
    );
    console.log(
      `watermark recorded path=${args.output} topics=${PROJECTOR_TOPICS.length} partitions=${partitionCount}`,
    );
  } finally {
    await admin.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error(`watermark failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
