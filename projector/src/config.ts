export interface ProjectorConfig {
  brokers: string[];
  schemaRegistryUrl: string;
  databaseUrl: string;
  groupId: string;
  bugSwapCoords: boolean;
  retentionEnabled: boolean;
  watermarkFile?: string;
}

type Env = Record<string, string | undefined>;

const DEFAULT_BROKERS = 'localhost:9092';
const DEFAULT_SCHEMA_REGISTRY_URL = 'http://localhost:8081';
const DEFAULT_DATABASE_URL = 'postgresql://gridpulse:gridpulse@localhost:5432/gridpulse';
const DEFAULT_GROUP_ID = 'gridpulse-projector';

function parseBrokers(raw: string | undefined): string[] {
  if (!raw || raw.trim() === '') {
    return [DEFAULT_BROKERS];
  }

  const brokers = raw
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);

  if (brokers.length === 0) {
    throw new Error('BROKERS must contain at least one broker');
  }
  return brokers;
}

function parseBoolean(raw: string | undefined, fallback: boolean, key: string): boolean {
  if (raw === undefined || raw === '') {
    return fallback;
  }
  if (raw === '1' || raw.toLowerCase() === 'true') {
    return true;
  }
  if (raw === '0' || raw.toLowerCase() === 'false') {
    return false;
  }
  throw new Error(`${key} must be 1, 0, true, or false`);
}

export function loadConfig(env: Env = process.env): ProjectorConfig {
  return {
    brokers: parseBrokers(env.BROKERS),
    schemaRegistryUrl: env.SCHEMA_REGISTRY_URL || DEFAULT_SCHEMA_REGISTRY_URL,
    databaseUrl: env.DATABASE_URL || DEFAULT_DATABASE_URL,
    groupId: env.GROUP_ID || DEFAULT_GROUP_ID,
    bugSwapCoords: parseBoolean(env.BUG_SWAP_COORDS, false, 'BUG_SWAP_COORDS'),
    retentionEnabled: parseBoolean(env.RETENTION_ENABLED, true, 'RETENTION_ENABLED'),
    watermarkFile: env.WATERMARK_FILE || undefined,
  };
}
