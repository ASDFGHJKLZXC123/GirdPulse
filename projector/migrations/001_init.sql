CREATE TABLE IF NOT EXISTS vehicles (
  id TEXT PRIMARY KEY,
  region TEXT NOT NULL,
  status TEXT NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicle_positions (
  vehicle_id TEXT PRIMARY KEY,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  speed_kph DOUBLE PRECISION NOT NULL,
  heading_deg DOUBLE PRECISION NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicle_events (
  event_id UUID PRIMARY KEY,
  vehicle_id TEXT NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  speed_kph DOUBLE PRECISION NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vehicle_events
  ON vehicle_events (vehicle_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS anomalies (
  id UUID PRIMARY KEY,
  vehicle_id TEXT NOT NULL,
  region TEXT NOT NULL,
  kind TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  detector_version INT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomalies
  ON anomalies (region, window_start DESC);

CREATE TABLE IF NOT EXISTS region_rollups (
  region TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  event_count BIGINT NOT NULL,
  active_vehicles INT NOT NULL,
  avg_speed_kph DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (region, window_start)
);

CREATE TABLE IF NOT EXISTS projector_offsets (
  topic TEXT NOT NULL,
  partition INT NOT NULL,
  last_offset BIGINT NOT NULL,
  PRIMARY KEY (topic, partition)
);
