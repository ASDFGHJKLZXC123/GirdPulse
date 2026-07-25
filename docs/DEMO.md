# M05 replay demo

This walkthrough demonstrates that Kafka is GridPulse's source of truth and PostgreSQL is a disposable read model:

1. build a correct PostgreSQL projection from a fixed Kafka prefix;
2. deliberately swap latitude and longitude while rebuilding it;
3. observe the broken coordinates;
4. rebuild without the bug; and
5. prove that the repaired projection exactly matches the original.

## Where to inspect the result

M05 does not have a web page. Redpanda Console at `http://localhost:8080` displays Kafka topics, not the PostgreSQL projection.

The replay evidence is written to:

- `.run/control-checksum.json` — the original clean projection;
- `.run/corrupt-checksum.json` — the intentionally broken projection;
- `.run/final-checksum.json` — the repaired projection.

Each file contains per-table row counts and hashes, a combined `digest`, and a five-vehicle `position_sample`.

## Safety

Replay deletes and rebuilds only these disposable tables in the local `gridpulse` database:

- `vehicles`
- `vehicle_positions`
- `vehicle_events`
- `anomalies`
- `region_rollups`
- `projector_offsets`

It does not delete Kafka topics or retained Kafka records. The replay command refuses a database other than `gridpulse`, refuses a non-local host unless explicitly overridden, and requires the `public` schema.

## Prerequisites

From the repository root:

```bash
pnpm install
make up topics schemas
make sim jobs projector
```

Let the pipeline run long enough to produce a useful sample. The demo stops the simulator and both stream jobs before capturing its fixed watermark; Redpanda and PostgreSQL remain running.

The replay demo itself works with a smaller sample. The crash-recovery proof additionally requires at least 10,000 projected messages in the fixed prefix because that is its intentional interruption point. With the default 50 vehicles and one-second tick, let a fresh pipeline run for at least four minutes before capturing the watermark.

## Automated walkthrough

Run:

```bash
make replay-demo
```

The target performs the entire clean → corrupt → clean sequence:

1. stop the three producers and capture all 15 terminal partition offsets in `.run/watermark.json`;
2. rebuild cleanly to that watermark and write `.run/control-checksum.json`;
3. rebuild the same Kafka prefix with `BUG_SWAP_COORDS=1` and write `.run/corrupt-checksum.json`;
4. require the corrupt digest to differ from the clean control;
5. rebuild the same prefix again with the bug disabled and write `.run/final-checksum.json`; and
6. require the final digest to equal the control digest exactly.

Success ends with:

```text
replay-demo: PASS clean replay exactly matches the fixed-watermark control
```

## Read the evidence

Run this from the repository root:

```bash
jq -s '{
  clean: .[0].position_sample[0],
  corrupt: .[1].position_sample[0],
  repaired: .[2].position_sample[0],
  corruption_detected: (.[0].digest != .[1].digest),
  repaired_matches_clean: (.[0].digest == .[2].digest)
}' \
  .run/control-checksum.json \
  .run/corrupt-checksum.json \
  .run/final-checksum.json
```

Expected shape:

```json
{
  "clean": {
    "vehicle_id": "veh-0001",
    "lat": 47.5,
    "lon": -122.4
  },
  "corrupt": {
    "vehicle_id": "veh-0001",
    "lat": -122.4,
    "lon": 47.5
  },
  "repaired": {
    "vehicle_id": "veh-0001",
    "lat": 47.5,
    "lon": -122.4
  },
  "corruption_detected": true,
  "repaired_matches_clean": true
}
```

The exact coordinates and digests depend on the captured Kafka prefix. The relationships must be the same:

- Clean latitude `47.5` and longitude `-122.4` describe a valid Seattle location.
- Corrupt latitude `-122.4` is impossible because latitude must be between `-90` and `90`; the two coordinates were visibly swapped.
- `corruption_detected: true` proves the bad projection differs.
- `repaired_matches_clean: true` proves replay restored the original projection exactly.

Only the coordinate-bearing `vehicle_positions` and `vehicle_events` table hashes should change during the corrupt pass. The `vehicles`, `anomalies`, and `region_rollups` hashes should remain unchanged.

## Pause on the corrupt database

Use the expanded sequence when you want to inspect PostgreSQL before repairing it:

```bash
scripts/watermark.sh .run/watermark.json

scripts/replay.sh \
  --from-beginning \
  --to-watermark .run/watermark.json
pnpm --dir projector exec tsx src/tools/checksum.ts \
  --write "$PWD/.run/control-checksum.json"

scripts/demo-corrupt.sh \
  --to-watermark .run/watermark.json
pnpm --dir projector exec tsx src/tools/checksum.ts \
  --write "$PWD/.run/corrupt-checksum.json" \
  --expect-different "$PWD/.run/control-checksum.json"
```

At this point PostgreSQL contains the intentionally corrupted projection. Inspect it with:

```bash
psql postgresql://gridpulse:gridpulse@localhost:5432/gridpulse \
  -c 'SELECT vehicle_id, lat, lon, updated_at
      FROM vehicle_positions
      ORDER BY vehicle_id
      LIMIT 5;'
```

Then repair and verify:

```bash
scripts/replay.sh \
  --from-beginning \
  --to-watermark .run/watermark.json
pnpm --dir projector exec tsx src/tools/checksum.ts \
  --write "$PWD/.run/final-checksum.json" \
  --expect "$PWD/.run/control-checksum.json"
```

The final checksum command exits successfully only when the repaired projection matches the original control.

## Crash-recovery proof

After a successful `make replay-demo` whose fixed prefix contains at least 10,000 projected messages, run:

```bash
make verify-crash-recovery
```

This test:

1. starts another clean fixed-watermark replay;
2. waits until at least 10,000 messages have committed;
3. sends `SIGKILL` only to the verified projector process;
4. proves that the partial projection differs from the control;
5. restarts without truncating the partial projection; and
6. proves that the resumed result exactly matches the uninterrupted control.

Success ends with:

```text
crash-recovery: PASS resumed projection exactly matches the uninterrupted control
```

## Cleanup

The bounded replay processes exit automatically at the watermark. Stop any other host application processes with:

```bash
make stop-apps
```

This leaves the infrastructure and retained Kafka records available for the next milestone.
