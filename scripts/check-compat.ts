import { access, readFile, readdir } from 'node:fs/promises';
import { basename, isAbsolute, join, resolve } from 'node:path';

// Canonical M01 contract: only register-schemas.ts registers new schema versions.
// Upstream producers/consumers must set `auto.register.schemas=false`.
// Filename→subject mapping: vehicle-event→fleet.vehicle-events-value,
// anomaly→fleet.anomalies-value, region-rollup→fleet.rollups.region-1m-value.
// Consumers must pin an explicit reader schema file; no directory-level schema globs.

const REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
const SCHEMA_DIR = resolve(
  process.env.SCHEMA_DIR ??
    (basename(process.cwd()) === 'scripts'
      ? join(process.cwd(), '..', 'schemas')
      : join(process.cwd(), 'schemas')),
);
const REPO_ROOT = basename(process.cwd()) === 'scripts'
  ? resolve(process.cwd(), '..')
  : process.cwd();

type CompatibilityResponse = {
  is_compatible: boolean;
  messages: string[];
};

const mappings = new Map<string, string>([
  ['vehicle-event', 'fleet.vehicle-events-value'],
  ['anomaly', 'fleet.anomalies-value'],
  ['region-rollup', 'fleet.rollups.region-1m-value'],
]);

function subjectForFile(file: string): string {
  const normalized = file.split(/[/\\]/).pop() ?? '';
  const dot = normalized.indexOf('.');
  if (dot === -1) {
    throw new Error(`invalid schema filename '${file}'`);
  }
  const prefix = normalized.substring(0, dot);
  const subject = mappings.get(prefix);
  if (!subject) {
    throw new Error(`unmapped schema filename prefix '${prefix}' in '${file}'`);
  }
  return subject;
}

async function allSchemaFiles(): Promise<string[]> {
  const all = await readdir(SCHEMA_DIR);
  return all
    .filter((name) => name.endsWith('.avsc'))
    .map((name) => join(SCHEMA_DIR, name));
}

async function resolveSchemaPath(file: string): Promise<string> {
  if (isAbsolute(file)) {
    return file;
  }

  const repoRelative = resolve(REPO_ROOT, file);
  const cwdRelative = resolve(process.cwd(), file);

  try {
    await access(repoRelative);
    return repoRelative;
  } catch {
    // If it's not under repo root, fall back to repo-relative command's cwd.
  }

  await access(cwdRelative);
  return cwdRelative;
}

async function fetchCompat(file: string, subject: string): Promise<boolean> {
  const schemaPath = await resolveSchemaPath(file);
  const schema = await readFile(schemaPath, 'utf-8');
  const response = await fetch(
    `${REGISTRY_URL}/compatibility/subjects/${encodeURIComponent(subject)}/versions/latest`,
    {
      method: 'POST',
      headers: {
        Accept: 'application/vnd.schemaregistry.v1+json',
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
      },
      body: JSON.stringify({ schema }),
    },
  );

  if (response.status === 404) {
    return true;
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `compatibility check failed for ${file} (${subject}): ${response.status} ${response.statusText}: ${text}`,
    );
  }

  const payload = (await response.json()) as CompatibilityResponse;
  if (payload.is_compatible) {
    return true;
  }

  console.error(`compatibility failed for ${file} against ${subject}`);
  console.error(payload.messages?.join('\n') ?? 'no detailed compatibility messages');
  return false;
}

async function main() {
  const args = process.argv.slice(2);
  const files = args.length > 0 ? args : await allSchemaFiles();

  let failed = false;
  for (const file of files) {
    if (!file.endsWith('.avsc')) {
      continue;
    }
    const subject = subjectForFile(file);
    const compatible = await fetchCompat(file, subject);
    if (!compatible) {
      failed = true;
    }
  }

  if (failed) {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
