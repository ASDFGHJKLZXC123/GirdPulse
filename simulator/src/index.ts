import { Kafka, Partitioners } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { SimulatorEngine, loadConfig } from './simulator-core';

const LOG_PREFIX = 'simulator';
const TOPIC = 'fleet.vehicle-events';
const SUBJECT = 'fleet.vehicle-events-value';
const READINESS_LOG = 'ready: first event produced';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function normalizeSchemaRegistryHost(raw: string): string {
  return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

function sanitizeStringList(values: string[]): string {
  return values.map((value) => `"${value}"`).join(', ');
}

async function run(): Promise<void> {
  const config = loadConfig();
  console.log(
    `${LOG_PREFIX} config ` +
      `brokers=${sanitizeStringList(config.brokers)} ` +
      `schemaRegistryUrl=${config.schemaRegistryUrl} ` +
      `vehicles=${config.vehicles} ` +
      `tickMs=${config.tickMs} ` +
      `seed=${config.seed} ` +
      `inject=${Array.from(config.inject).join(',')}`,
  );

  const engine = new SimulatorEngine(config);
  const registry = new SchemaRegistry({ host: normalizeSchemaRegistryHost(config.schemaRegistryUrl) });
  const schemaId = await registry.getLatestSchemaId(SUBJECT);
  const kafka = new Kafka({
    clientId: 'gridpulse-simulator',
    brokers: config.brokers,
  });
  const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();

  let running = true;
  let firstEmitLogged = false;

  const shutdown = async (): Promise<void> => {
    running = false;
    try {
      await producer.disconnect();
    } finally {
      process.exit(0);
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  const waitMs = config.tickMs;

  while (running) {
    const now = Date.now();
    const { events } = engine.tick(now);
    if (events.length > 0) {
      const encoded = await Promise.all(
        events.map(async (event) => ({
          key: event.vehicle_id,
          value: await registry.encode(schemaId, event),
        })),
      );

      await producer.send({
        topic: TOPIC,
        messages: encoded,
      });

      if (!firstEmitLogged) {
        console.log(READINESS_LOG);
        firstEmitLogged = true;
      }
    }

    const tickDuration = Date.now() - now;
    const delay = Math.max(0, waitMs - tickDuration);
    await sleep(delay);
  }
}

run().catch((error) => {
  console.error(`${LOG_PREFIX} startup failed: ${String(error instanceof Error ? error.message : error)}`);
  process.exit(1);
});
