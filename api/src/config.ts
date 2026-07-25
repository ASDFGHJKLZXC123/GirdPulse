export interface ApiConfig {
  port: number;
  databaseUrl: string;
  brokers: string[];
  schemaRegistryUrl: string;
}

type Env = Record<string, string | undefined>;

const DEFAULT_PORT = 4000;
const DEFAULT_DATABASE_URL = 'postgresql://gridpulse:gridpulse@localhost:5432/gridpulse';
const DEFAULT_BROKERS = 'localhost:9092';
const DEFAULT_SCHEMA_REGISTRY_URL = 'http://localhost:8081';

function parsePort(raw: string | undefined): number {
  if (raw === undefined || raw.trim() === '') {
    return DEFAULT_PORT;
  }

  const port = Number(raw);
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new Error('PORT must be an integer between 0 and 65535');
  }
  return port;
}

function parseBrokers(raw: string | undefined): string[] {
  const source = raw === undefined || raw.trim() === '' ? DEFAULT_BROKERS : raw;
  const brokers = source
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);

  if (brokers.length === 0) {
    throw new Error('BROKERS must contain at least one broker');
  }
  return brokers;
}

export function loadConfig(env: Env = process.env): ApiConfig {
  return {
    port: parsePort(env.PORT),
    databaseUrl: env.DATABASE_URL || DEFAULT_DATABASE_URL,
    brokers: parseBrokers(env.BROKERS),
    schemaRegistryUrl: env.SCHEMA_REGISTRY_URL || DEFAULT_SCHEMA_REGISTRY_URL,
  };
}
