import { createHash } from 'node:crypto';
import { mkdir, readFile, rename, unlink, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { Pool } from 'pg';
import { loadConfig } from '../config.js';

interface Arguments {
  write?: string;
  expect?: string;
  expectDifferent?: string;
}

interface TableChecksum {
  rows: number;
  sha256: string;
}

interface ChecksumReport {
  version: 1;
  algorithm: 'sha256';
  tables: Record<string, TableChecksum>;
  digest: string;
  position_sample: Array<{
    vehicle_id: string;
    lat: number;
    lon: number;
  }>;
}

const TABLE_QUERIES = [
  {
    name: 'vehicles',
    sql: 'SELECT row_to_json(v)::text AS row FROM vehicles v ORDER BY id',
  },
  {
    name: 'vehicle_positions',
    sql: 'SELECT row_to_json(v)::text AS row FROM vehicle_positions v ORDER BY vehicle_id',
  },
  {
    name: 'vehicle_events',
    sql: 'SELECT row_to_json(v)::text AS row FROM vehicle_events v ORDER BY event_id',
  },
  {
    name: 'anomalies',
    sql: 'SELECT row_to_json(v)::text AS row FROM anomalies v ORDER BY id',
  },
  {
    name: 'region_rollups',
    sql: `
      SELECT row_to_json(v)::text AS row
      FROM region_rollups v
      ORDER BY region, window_start
    `,
  },
] as const;

function usage(): never {
  throw new Error(
    'usage: checksum.ts [--write <file>] [--expect <file> | --expect-different <file>]',
  );
}

function parseArguments(argv: string[]): Arguments {
  const parsed: Arguments = {};
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const value = argv[index + 1];
    if (argument === '--write' && value) {
      parsed.write = resolve(value);
      index += 1;
    } else if (argument === '--expect' && value) {
      parsed.expect = resolve(value);
      index += 1;
    } else if (argument === '--expect-different' && value) {
      parsed.expectDifferent = resolve(value);
      index += 1;
    } else {
      usage();
    }
  }
  if (parsed.expect && parsed.expectDifferent) {
    usage();
  }
  return parsed;
}

function updateFramed(hash: ReturnType<typeof createHash>, value: string): void {
  hash.update(String(Buffer.byteLength(value, 'utf8')));
  hash.update(':');
  hash.update(value);
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

async function expectedDigest(path: string): Promise<string> {
  const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('digest' in parsed) ||
    typeof parsed.digest !== 'string' ||
    !/^[a-f0-9]{64}$/.test(parsed.digest)
  ) {
    throw new Error(`Invalid checksum file: ${path}`);
  }
  return parsed.digest;
}

async function buildReport(pool: Pool): Promise<ChecksumReport> {
  const globalHash = createHash('sha256');
  const tables: Record<string, TableChecksum> = {};
  await pool.query("SET TIME ZONE 'UTC'");

  for (const table of TABLE_QUERIES) {
    const result = await pool.query<{ row: string }>(table.sql);
    const tableHash = createHash('sha256');
    for (const record of result.rows) {
      updateFramed(tableHash, record.row);
    }
    const digest = tableHash.digest('hex');
    tables[table.name] = { rows: result.rowCount ?? result.rows.length, sha256: digest };
    updateFramed(globalHash, table.name);
    updateFramed(globalHash, digest);
  }

  const samples = await pool.query<{
    vehicle_id: string;
    lat: number;
    lon: number;
  }>(
    `SELECT vehicle_id, lat, lon
     FROM vehicle_positions
     ORDER BY vehicle_id
     LIMIT 5`,
  );

  return {
    version: 1,
    algorithm: 'sha256',
    tables,
    digest: globalHash.digest('hex'),
    position_sample: samples.rows,
  };
}

async function main(): Promise<void> {
  const args = parseArguments(process.argv.slice(2));
  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl, max: 1 });
  let report: ChecksumReport;
  try {
    report = await buildReport(pool);
  } finally {
    await pool.end();
  }

  if (args.write) {
    await writeJsonAtomically(args.write, report);
  }

  if (args.expect) {
    const expected = await expectedDigest(args.expect);
    if (report.digest !== expected) {
      throw new Error(`checksum mismatch expected=${expected} actual=${report.digest}`);
    }
    console.log(`checksum matches expected=${args.expect}`);
  }

  if (args.expectDifferent) {
    const expected = await expectedDigest(args.expectDifferent);
    if (report.digest === expected) {
      throw new Error(`checksum unexpectedly matches ${args.expectDifferent}`);
    }
    console.log(`checksum differs from control=${args.expectDifferent}`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((error: unknown) => {
  console.error(`checksum failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
