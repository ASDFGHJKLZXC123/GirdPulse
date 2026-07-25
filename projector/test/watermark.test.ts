import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { offsetKey } from '../src/offsets.js';
import { ANOMALIES_TOPIC, REGION_ROLLUPS_TOPIC, VEHICLE_EVENTS_TOPIC } from '../src/types.js';
import { canonicalReplayPartitions, loadWatermark, nextReplayPartition } from '../src/watermark.js';

const temporaryDirectories: string[] = [];

function partitions(count: number, offset = '10'): Record<string, string> {
  return Object.fromEntries(
    Array.from({ length: count }, (_, partition) => [String(partition), offset]),
  );
}

function validWatermark(): Record<string, unknown> {
  return {
    version: 1,
    captured_at: '2026-07-24T12:00:00.000Z',
    topics: {
      [VEHICLE_EVENTS_TOPIC]: partitions(6),
      [ANOMALIES_TOPIC]: partitions(6),
      [REGION_ROLLUPS_TOPIC]: partitions(3),
    },
    low_offsets: {
      [VEHICLE_EVENTS_TOPIC]: partitions(6, '0'),
      [ANOMALIES_TOPIC]: partitions(6, '0'),
      [REGION_ROLLUPS_TOPIC]: partitions(3, '0'),
    },
  };
}

async function writeWatermark(value: unknown): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), 'gridpulse-watermark-'));
  temporaryDirectories.push(directory);
  const path = join(directory, 'watermark.json');
  await writeFile(path, JSON.stringify(value), 'utf8');
  return path;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, {
        recursive: true,
        force: true,
      }),
    ),
  );
});

describe('fixed watermark validation', () => {
  it('loads exactly the canonical 6/6/3 partition set', async () => {
    const targets = await loadWatermark(await writeWatermark(validWatermark()));

    expect(targets.size).toBe(15);
    expect(targets.get(offsetKey(VEHICLE_EVENTS_TOPIC, 5))).toBe('10');
    expect(targets.get(offsetKey(ANOMALIES_TOPIC, 5))).toBe('10');
    expect(targets.get(offsetKey(REGION_ROLLUPS_TOPIC, 2))).toBe('10');
  });

  it('rejects a missing canonical partition', async () => {
    const watermark = validWatermark();
    const topics = watermark.topics as Record<string, Record<string, string>>;
    delete topics[ANOMALIES_TOPIC]['5'];

    await expect(loadWatermark(await writeWatermark(watermark))).rejects.toThrow(
      'must contain exactly partitions',
    );
  });

  it('rejects targets below the empty-partition sentinel', async () => {
    const watermark = validWatermark();
    const topics = watermark.topics as Record<string, Record<string, string>>;
    topics[REGION_ROLLUPS_TOPIC]['0'] = '-2';

    await expect(loadWatermark(await writeWatermark(watermark))).rejects.toThrow(
      'must be at least -1',
    );
  });

  it('requires captured low offsets for retention-safe replays', async () => {
    const watermark = validWatermark();
    delete watermark.low_offsets;

    await expect(loadWatermark(await writeWatermark(watermark))).rejects.toThrow(
      'Invalid watermark file',
    );
  });

  it('rejects a target below its captured low offset', async () => {
    const watermark = validWatermark();
    const topics = watermark.topics as Record<string, Record<string, string>>;
    const lowOffsets = watermark.low_offsets as Record<string, Record<string, string>>;
    topics[VEHICLE_EVENTS_TOPIC]['0'] = '4';
    lowOffsets[VEHICLE_EVENTS_TOPIC]['0'] = '5';

    await expect(loadWatermark(await writeWatermark(watermark))).rejects.toThrow(
      'is below captured low offset',
    );
  });

  it('processes fixed-watermark partitions in one canonical order', async () => {
    const targets = await loadWatermark(await writeWatermark(validWatermark()));
    targets.set(offsetKey(ANOMALIES_TOPIC, 1), '-1');
    const ordered = canonicalReplayPartitions({
      [REGION_ROLLUPS_TOPIC]: [2, 0, 1],
      [VEHICLE_EVENTS_TOPIC]: [5, 2, 0, 4, 1, 3],
      [ANOMALIES_TOPIC]: [5, 1, 4, 0, 3, 2],
    });

    expect(ordered).toEqual([
      { topic: VEHICLE_EVENTS_TOPIC, partition: 0 },
      { topic: VEHICLE_EVENTS_TOPIC, partition: 1 },
      { topic: VEHICLE_EVENTS_TOPIC, partition: 2 },
      { topic: VEHICLE_EVENTS_TOPIC, partition: 3 },
      { topic: VEHICLE_EVENTS_TOPIC, partition: 4 },
      { topic: VEHICLE_EVENTS_TOPIC, partition: 5 },
      { topic: ANOMALIES_TOPIC, partition: 0 },
      { topic: ANOMALIES_TOPIC, partition: 1 },
      { topic: ANOMALIES_TOPIC, partition: 2 },
      { topic: ANOMALIES_TOPIC, partition: 3 },
      { topic: ANOMALIES_TOPIC, partition: 4 },
      { topic: ANOMALIES_TOPIC, partition: 5 },
      { topic: REGION_ROLLUPS_TOPIC, partition: 0 },
      { topic: REGION_ROLLUPS_TOPIC, partition: 1 },
      { topic: REGION_ROLLUPS_TOPIC, partition: 2 },
    ]);
    expect(nextReplayPartition(ordered, targets, new Map())).toEqual({
      topic: VEHICLE_EVENTS_TOPIC,
      partition: 0,
    });

    const progress = new Map([
      [offsetKey(VEHICLE_EVENTS_TOPIC, 0), '10'],
      [offsetKey(VEHICLE_EVENTS_TOPIC, 1), '10'],
      [offsetKey(VEHICLE_EVENTS_TOPIC, 2), '10'],
      [offsetKey(VEHICLE_EVENTS_TOPIC, 3), '10'],
      [offsetKey(VEHICLE_EVENTS_TOPIC, 4), '10'],
      [offsetKey(VEHICLE_EVENTS_TOPIC, 5), '10'],
      [offsetKey(ANOMALIES_TOPIC, 0), '10'],
      [offsetKey(ANOMALIES_TOPIC, 2), '10'],
      [offsetKey(ANOMALIES_TOPIC, 3), '10'],
      [offsetKey(ANOMALIES_TOPIC, 4), '10'],
      [offsetKey(ANOMALIES_TOPIC, 5), '10'],
      [offsetKey(REGION_ROLLUPS_TOPIC, 0), '10'],
      [offsetKey(REGION_ROLLUPS_TOPIC, 1), '10'],
    ]);
    expect(nextReplayPartition(ordered, targets, progress)).toEqual({
      topic: REGION_ROLLUPS_TOPIC,
      partition: 2,
    });
  });

  it('rejects an invalid replay assignment', () => {
    expect(() =>
      canonicalReplayPartitions({
        [VEHICLE_EVENTS_TOPIC]: [0, 1, 2, 3, 4],
        [ANOMALIES_TOPIC]: [0, 1, 2, 3, 4, 5],
        [REGION_ROLLUPS_TOPIC]: [0, 1, 2],
      }),
    ).toThrow('requires exclusive assignment');
  });
});
