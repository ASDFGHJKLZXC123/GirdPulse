import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import type { Pool } from 'pg';

const DEFAULT_MIGRATION_DIR = fileURLToPath(new URL('../migrations', import.meta.url));

export async function runMigrations(
  pool: Pool,
  migrationDir = DEFAULT_MIGRATION_DIR,
): Promise<void> {
  const files = (await readdir(migrationDir))
    .filter((file) => /^\d+.*\.sql$/.test(file))
    .sort((left, right) => left.localeCompare(right));

  if (files.length === 0) {
    throw new Error(`No SQL migrations found in ${migrationDir}`);
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const file of files) {
      const sql = await readFile(`${migrationDir}/${file}`, 'utf8');
      await client.query(sql);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
