import { Kafka, logLevel, type Admin } from 'kafkajs';
import { Pool, type PoolClient } from 'pg';
import { loadConfig } from '../config.js';
import { runMigrations } from '../migrations.js';
import { PROJECTOR_PARTITION_COUNTS, PROJECTOR_TOPICS, type ProjectorTopic } from '../types.js';
import { loadWatermarkFile, type WatermarkFile } from '../watermark.js';

const PROJECTION_TABLES = [
  'public.vehicles',
  'public.vehicle_positions',
  'public.vehicle_events',
  'public.anomalies',
  'public.region_rollups',
] as const;

type StartMode =
  { kind: 'from-beginning' } | { kind: 'since'; timestamp: number; isoTimestamp: string };

interface Arguments {
  mode: StartMode;
  watermarkFile?: string;
}

interface OffsetSeed {
  topic: ProjectorTopic;
  partition: number;
  lastOffset: string;
}

interface TopicRange {
  low: string;
  high: string;
}

function usage(): never {
  throw new Error(
    'usage: seed-since.ts (--from-beginning | --since <ISO8601>) [--watermark <file>] --confirm-projection-rebuild',
  );
}

function parseIsoTimestamp(raw: string | undefined): { timestamp: number; isoTimestamp: string } {
  if (!raw || !raw.includes('T') || !/(?:Z|[+-]\d{2}:\d{2})$/i.test(raw)) {
    throw new Error('--since must be an ISO8601 timestamp with an explicit timezone');
  }
  const timestamp = Date.parse(raw);
  if (!Number.isFinite(timestamp)) {
    throw new Error(`Invalid --since timestamp: ${raw}`);
  }
  return { timestamp, isoTimestamp: new Date(timestamp).toISOString() };
}

function parseArguments(argv: string[]): Arguments {
  let mode: StartMode | undefined;
  let watermarkFile: string | undefined;
  let confirmed = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--from-beginning') {
      if (mode) {
        usage();
      }
      mode = { kind: 'from-beginning' };
    } else if (argument === '--since') {
      if (mode) {
        usage();
      }
      const parsed = parseIsoTimestamp(argv[index + 1]);
      mode = { kind: 'since', ...parsed };
      index += 1;
    } else if (argument === '--watermark') {
      if (watermarkFile || !argv[index + 1]) {
        usage();
      }
      watermarkFile = argv[index + 1];
      index += 1;
    } else if (argument === '--confirm-projection-rebuild') {
      confirmed = true;
    } else {
      usage();
    }
  }

  if (!mode || !confirmed) {
    usage();
  }
  return { mode, watermarkFile };
}

function assertSafeDatabaseTarget(connectionString: string): void {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    throw new Error('DATABASE_URL must be a PostgreSQL URL');
  }

  if (url.protocol !== 'postgresql:' && url.protocol !== 'postgres:') {
    throw new Error('DATABASE_URL must use the postgres or postgresql scheme');
  }
  if (url.search !== '') {
    throw new Error('Refusing DATABASE_URL query parameters for a destructive replay target');
  }

  const localHosts = new Set(['localhost', '127.0.0.1', '[::1]']);
  if (!localHosts.has(url.hostname) && process.env.ALLOW_REMOTE_REPLAY !== '1') {
    throw new Error(
      `Refusing to rebuild a non-local database host (${url.hostname}); set ALLOW_REMOTE_REPLAY=1 only for an intentional disposable target`,
    );
  }

  if (decodeURIComponent(url.pathname.replace(/^\//, '')) !== 'gridpulse') {
    throw new Error('Refusing to rebuild a database other than gridpulse');
  }
}

async function assertSafeConnectedDatabase(client: PoolClient): Promise<void> {
  const result = await client.query<{
    database_name: string;
    schema_name: string | null;
  }>(`
    SELECT
      current_database()::text AS database_name,
      current_schema()::text AS schema_name
  `);
  const connected = result.rows[0];
  if (connected?.database_name !== 'gridpulse') {
    throw new Error(
      `Refusing connected database ${connected?.database_name ?? 'unknown'}; expected gridpulse`,
    );
  }
  if (connected.schema_name !== 'public') {
    throw new Error(
      `Refusing connected schema ${connected.schema_name ?? 'unknown'}; expected public`,
    );
  }
}

function assertCanonicalPartitions(
  topic: ProjectorTopic,
  partitions: readonly number[],
  source: string,
): void {
  const expected = Array.from(
    { length: PROJECTOR_PARTITION_COUNTS[topic] },
    (_, partition) => partition,
  );
  const actual = [...partitions].sort((left, right) => left - right);
  if (
    actual.length !== expected.length ||
    actual.some((partition, index) => partition !== expected[index])
  ) {
    throw new Error(
      `${source} ${topic} must contain exactly partitions ${expected.join(',')}; got ${actual.join(',')}`,
    );
  }
}

async function resolveOffsets(admin: Admin, timestamp: number): Promise<OffsetSeed[]> {
  const seeds: OffsetSeed[] = [];

  for (const topic of PROJECTOR_TOPICS) {
    const offsets = await admin.fetchTopicOffsetsByTimestamp(topic, timestamp);
    assertCanonicalPartitions(
      topic,
      offsets.map((offset) => offset.partition),
      'Kafka timestamp offsets for',
    );
    for (const offset of offsets) {
      if (
        !Number.isInteger(offset.partition) ||
        offset.partition < 0 ||
        !/^\d+$/.test(offset.offset)
      ) {
        throw new Error(`Kafka returned an invalid timestamp offset for ${topic}`);
      }
      seeds.push({
        topic,
        partition: offset.partition,
        lastOffset: (BigInt(offset.offset) - 1n).toString(),
      });
    }
  }

  return seeds;
}

async function fetchTopicRanges(admin: Admin): Promise<Map<string, TopicRange>> {
  const ranges = new Map<string, TopicRange>();
  for (const topic of PROJECTOR_TOPICS) {
    const offsets = await admin.fetchTopicOffsets(topic);
    assertCanonicalPartitions(
      topic,
      offsets.map((offset) => offset.partition),
      'Kafka offsets for',
    );
    for (const offset of offsets) {
      if (!/^\d+$/.test(offset.low) || !/^\d+$/.test(offset.high)) {
        throw new Error(
          `Kafka returned invalid low/high offsets for ${topic}[${offset.partition}]`,
        );
      }
      ranges.set(`${topic}:${offset.partition}`, {
        low: offset.low,
        high: offset.high,
      });
    }
  }
  return ranges;
}

function verifyWatermarkAvailability(
  watermark: WatermarkFile,
  ranges: ReadonlyMap<string, TopicRange>,
): void {
  for (const topic of PROJECTOR_TOPICS) {
    for (let partition = 0; partition < PROJECTOR_PARTITION_COUNTS[topic]; partition += 1) {
      const range = ranges.get(`${topic}:${partition}`);
      if (!range) {
        throw new Error(`Kafka range is missing ${topic}[${partition}]`);
      }

      const capturedLow = watermark.low_offsets[topic][String(partition)];
      const target = watermark.topics[topic][String(partition)];
      if (range.low !== capturedLow) {
        throw new Error(
          `Kafka low offset changed for ${topic}[${partition}] captured=${capturedLow} current=${range.low}; fixed-prefix replay is no longer reproducible`,
        );
      }
      if (target === '-1' && range.high !== range.low) {
        throw new Error(
          `Watermark marks nonempty ${topic}[${partition}] as empty (low=${range.low} high=${range.high})`,
        );
      }
      if (target !== '-1' && BigInt(range.high) - 1n < BigInt(target)) {
        throw new Error(
          `Kafka no longer contains watermark target ${topic}[${partition}]=${target}`,
        );
      }
    }
  }
}

async function replaceProjection(client: PoolClient, seeds: readonly OffsetSeed[]): Promise<void> {
  await client.query('BEGIN');
  try {
    await client.query(`TRUNCATE TABLE ${PROJECTION_TABLES.join(', ')}`);
    await client.query('DELETE FROM public.projector_offsets');
    for (const seed of seeds) {
      await client.query(
        `INSERT INTO public.projector_offsets (topic, partition, last_offset)
         VALUES ($1, $2, $3)`,
        [seed.topic, seed.partition, seed.lastOffset],
      );
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

async function main(): Promise<void> {
  const args = parseArguments(process.argv.slice(2));
  const { mode } = args;
  const config = loadConfig();
  assertSafeDatabaseTarget(config.databaseUrl);

  const kafka = new Kafka({
    clientId: 'gridpulse-replay-offsets',
    brokers: config.brokers,
    logLevel: logLevel.NOTHING,
  });
  const admin = kafka.admin();
  let seeds: OffsetSeed[] = [];
  await admin.connect();
  try {
    const ranges = await fetchTopicRanges(admin);
    if (args.watermarkFile) {
      const watermark = await loadWatermarkFile(args.watermarkFile);
      verifyWatermarkAvailability(watermark, ranges);
    }
    if (mode.kind === 'since') {
      seeds = await resolveOffsets(admin, mode.timestamp);
    }
  } finally {
    await admin.disconnect();
  }

  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: 1,
    options: '-c search_path=public',
  });
  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    await assertSafeConnectedDatabase(client);
    client.release();
    client = undefined;

    await runMigrations(pool);

    client = await pool.connect();
    await assertSafeConnectedDatabase(client);
    await replaceProjection(client, seeds);
  } finally {
    client?.release();
    await pool.end();
  }

  if (mode.kind === 'from-beginning') {
    console.log(
      `projection reset from beginning tables=${PROJECTION_TABLES.length} offsets=cleared`,
    );
  } else {
    console.log(
      `projection reset since=${mode.isoTimestamp} tables=${PROJECTION_TABLES.length} offset_rows=${seeds.length}`,
    );
  }
}

main().catch((error: unknown) => {
  console.error(
    `projection reset failed: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 1;
});
