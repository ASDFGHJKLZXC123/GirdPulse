import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';

describe('projector configuration', () => {
  it('enables retention by default', () => {
    expect(loadConfig({}).retentionEnabled).toBe(true);
  });

  it('lets deterministic replay disable retention explicitly', () => {
    expect(loadConfig({ RETENTION_ENABLED: '0' }).retentionEnabled).toBe(false);
  });
});
