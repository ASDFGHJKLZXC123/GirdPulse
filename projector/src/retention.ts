import type { Pool } from 'pg';

export async function applyRetention(pool: Pool): Promise<number> {
  const result = await pool.query(
    "DELETE FROM vehicle_events WHERE occurred_at < now() - interval '24 hours'",
  );
  return result.rowCount ?? 0;
}
