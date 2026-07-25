import type { Pool } from 'pg';
import { applyProjection } from './projections.js';
import type { DecodedBatch } from './types.js';

export interface BatchTransactionOptions {
  swapCoords: boolean;
  faultAfterProjections?: () => void | Promise<void>;
  heartbeat?: () => Promise<void>;
}

export interface BatchTransactionResult {
  processed: number;
  lastOffset: string | null;
}

export async function processDecodedBatch(
  pool: Pool,
  batch: DecodedBatch,
  options: BatchTransactionOptions,
): Promise<BatchTransactionResult> {
  if (batch.messages.length === 0) {
    return { processed: 0, lastOffset: null };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const [index, message] of batch.messages.entries()) {
      await applyProjection(client, batch.topic, message.value, {
        swapCoords: options.swapCoords,
      });
      if ((index + 1) % 25 === 0) {
        await options.heartbeat?.();
      }
    }

    await options.faultAfterProjections?.();

    const lastOffset = batch.messages.reduce(
      (maximum, message) => (BigInt(message.offset) > BigInt(maximum) ? message.offset : maximum),
      batch.messages[0].offset,
    );
    const offsetResult = await client.query<{ last_offset: string }>(
      `
        INSERT INTO projector_offsets (topic, partition, last_offset)
        VALUES ($1, $2, $3)
        ON CONFLICT (topic, partition) DO UPDATE SET
          last_offset = GREATEST(projector_offsets.last_offset, EXCLUDED.last_offset)
        RETURNING last_offset::text AS last_offset
      `,
      [batch.topic, batch.partition, lastOffset],
    );

    const storedOffset = offsetResult.rows[0]?.last_offset;
    if (storedOffset === undefined) {
      throw new Error('Postgres did not return the stored projector offset');
    }
    await client.query('COMMIT');
    return { processed: batch.messages.length, lastOffset: storedOffset };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
