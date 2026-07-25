import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { offsetKey } from '../src/offsets.js';
import { ANOMALIES_TOPIC, REGION_ROLLUPS_TOPIC, VEHICLE_EVENTS_TOPIC } from '../src/types.js';
import { loadWatermark } from '../src/watermark.js';

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
});
