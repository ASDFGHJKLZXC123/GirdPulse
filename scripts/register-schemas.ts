// Canonical M01 contract: all registration is performed only via this script.
// Upstream producers/consumers must set `auto.register.schemas=false` and rely on existing subjects.

import { readFile, readdir } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';

const REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
const SCHEMA_DIR = resolve(
  process.env.SCHEMA_DIR ??
    (basename(process.cwd()) === 'scripts' ? join(process.cwd(), '..', 'schemas') : join(process.cwd(), 'schemas')),
);

const mappings = [
  { prefix: 'vehicle-event', subject: 'fleet.vehicle-events-value' },
  { prefix: 'anomaly', subject: 'fleet.anomalies-value' },
  { prefix: 'region-rollup', subject: 'fleet.rollups.region-1m-value' },
] as const;

type Mapping = (typeof mappings)[number];

type SchemaFile = {
  file: string;
  version: number;
};

type SchemaRegistryResponse = {
  id: number;
};

type VersionResponse = {
  subject: string;
  version: number;
  id: number;
  schema: string;
};

type CompatibilityConfigResponse = {
  compatibility: string;
};

async function fetchJson<T>(
  url: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: {
      Accept: 'application/vnd.schemaregistry.v1+json',
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
      ...(init.headers ?? {}),
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `${init.method ?? 'GET'} ${url} → ${response.status} ${response.statusText}: ${text}`,
    );
  }

  return response.json() as Promise<T>;
}

function schemaVersionFromFile(prefix: string, filename: string): number {
  const match = new RegExp(`^${prefix}\\.v(\\d+)\\.avsc$`).exec(filename);
  if (!match) {
    throw new Error(`unrecognized schema file '${filename}' for prefix '${prefix}'`);
  }
  return Number(match[1]);
}

async function schemasByPrefix(
  prefix: string,
): Promise<SchemaFile[]> {
  const entries = await readdir(SCHEMA_DIR);
  const matching = entries
    .filter((entry) => new RegExp(`^${prefix}\\.v\\d+\\.avsc$`).test(entry))
    .map((file) => ({ file, version: schemaVersionFromFile(prefix, file) }))
    .sort((a, b) => a.version - b.version);
  return matching;
}

async function setCompatibility(subject: string): Promise<void> {
  await fetchJson<CompatibilityConfigResponse>(
    `${REGISTRY_URL}/config/${encodeURIComponent(subject)}`,
    {
      method: 'PUT',
      body: JSON.stringify({ compatibility: 'FULL' }),
    },
  );
}

async function registerSchema(
  subject: string,
  filename: string,
): Promise<number> {
  const schema = await readFile(join(SCHEMA_DIR, filename), 'utf-8');
  const response = await fetchJson<SchemaRegistryResponse>(
    `${REGISTRY_URL}/subjects/${encodeURIComponent(subject)}/versions`,
    {
      method: 'POST',
      body: JSON.stringify({ schema }),
    },
  );
  return response.id;
}

async function getSubjectVersions(subject: string): Promise<number[]> {
  const versions = await fetchJson<number[]>(
    `${REGISTRY_URL}/subjects/${encodeURIComponent(subject)}/versions`,
  );
  return versions;
}

async function getVersionInfo(subject: string, version: number): Promise<VersionResponse> {
  return fetchJson<VersionResponse>(
    `${REGISTRY_URL}/subjects/${encodeURIComponent(subject)}/versions/${version}`,
  );
}

async function processSubject(mapping: Mapping): Promise<void> {
  const matched = await schemasByPrefix(mapping.prefix);
  if (matched.length === 0) {
    return;
  }

  await setCompatibility(mapping.subject);

  for (const schema of matched) {
    await registerSchema(mapping.subject, schema.file);
  }

  const versions = await getSubjectVersions(mapping.subject);
  const entries = await Promise.all(
    versions.map(async (version) => {
      const versionInfo = await getVersionInfo(mapping.subject, version);
      return { version, id: versionInfo.id };
    }),
  );

  if (entries.length > 0) {
    const versionSummary = entries
      .map((entry) => `${entry.version}: ${entry.id}`)
      .join(', ');
    console.log(`${mapping.subject} -> [${versionSummary}]`);
  }
}

async function main() {
  for (const mapping of mappings) {
    await processSubject(mapping);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
