import type { Pool } from 'pg';

export function offsetKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}

export async function loadProjectorOffsets(pool: Pool): Promise<Map<string, string>> {
  const result = await pool.query<{
    topic: string;
    partition: number;
    last_offset: string;
  }>('SELECT topic, partition, last_offset::text FROM projector_offsets');

  return new Map(result.rows.map((row) => [offsetKey(row.topic, row.partition), row.last_offset]));
}
