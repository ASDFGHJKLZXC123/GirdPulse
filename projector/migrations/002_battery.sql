-- Additive, nullable column for the VehicleEvent v2 battery_pct field.
-- IF NOT EXISTS keeps this restart-safe: runMigrations re-executes every SQL
-- migration on each projector start, so a bare ADD COLUMN would fail after the
-- first run. No DEFAULT and no backfill: rows written by v1 events stay NULL.
ALTER TABLE vehicle_positions
  ADD COLUMN IF NOT EXISTS battery_pct DOUBLE PRECISION;
