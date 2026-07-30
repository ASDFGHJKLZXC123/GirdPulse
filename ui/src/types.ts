export const REGION_CODES = ['SEA', 'SFO', 'NYC', 'AUS'] as const;

export type RegionCode = (typeof REGION_CODES)[number];
export type RegionSelection = 'ALL' | RegionCode;
export type VehicleStatus = 'ACTIVE' | 'IDLE' | 'OFFLINE';
export type AnomalyKind = 'SPEED_THRESHOLD' | 'SPEED_ZSCORE';
export type DateTime = string;

export interface Position {
  lat: number;
  lon: number;
  speedKph: number;
  headingDeg: number;
  updatedAt: DateTime;
}

export interface Vehicle {
  id: string;
  region: RegionCode;
  status: VehicleStatus;
  lastSeen: DateTime;
  position: Position;
}

export interface VehicleEvent {
  eventId: string;
  vehicleId: string;
  lat: number;
  lon: number;
  speedKph: number;
  occurredAt: DateTime;
}

export interface Anomaly {
  id: string;
  vehicleId: string;
  region: RegionCode;
  kind: AnomalyKind;
  value: number;
  detectorVersion: number;
  windowStart: DateTime;
  windowEnd: DateTime;
}

export interface RegionRollup {
  region: RegionCode;
  windowStart: DateTime;
  windowEnd: DateTime;
  eventCount: number;
  activeVehicles: number;
  avgSpeedKph: number;
}

export interface VehiclesQueryData {
  vehicles: Vehicle[];
}

export interface VehiclesQueryVariables {
  region: RegionCode | null;
}

export interface AnomaliesQueryData {
  anomalies: Anomaly[];
}

export interface AnomaliesQueryVariables {
  region: RegionCode | null;
  since: DateTime;
}

export interface RegionRollupsQueryData {
  regionRollups: RegionRollup[];
}

export interface RegionRollupsQueryVariables {
  region: RegionCode;
  from: DateTime;
  to: DateTime;
}

export interface VehicleMovedSubscriptionData {
  vehicleMoved: VehicleEvent;
}

export interface VehicleMovedSubscriptionVariables {
  region: RegionCode | null;
}

export interface AnomalyDetectedSubscriptionData {
  anomalyDetected: Anomaly;
}

export interface AnomalyDetectedSubscriptionVariables {
  region: RegionCode | null;
}

export interface DateTimeRange {
  from: DateTime;
  to: DateTime;
}
