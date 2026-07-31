import { gql } from '@apollo/client';

export const VEHICLES_QUERY = gql`
  query Vehicles($region: String) {
    vehicles(region: $region) {
      id
      region
      status
      lastSeen
      position {
        lat
        lon
        speedKph
        headingDeg
        batteryPct
        updatedAt
      }
    }
  }
`;

export const ANOMALIES_QUERY = gql`
  query Anomalies($region: String, $since: DateTime!) {
    anomalies(region: $region, since: $since) {
      id
      vehicleId
      region
      kind
      value
      detectorVersion
      windowStart
      windowEnd
    }
  }
`;

export const REGION_ROLLUPS_QUERY = gql`
  query RegionRollups($region: String!, $from: DateTime!, $to: DateTime!) {
    regionRollups(region: $region, from: $from, to: $to) {
      region
      windowStart
      windowEnd
      eventCount
      activeVehicles
      avgSpeedKph
    }
  }
`;

export const VEHICLE_MOVED_SUBSCRIPTION = gql`
  subscription VehicleMoved($region: String) {
    vehicleMoved(region: $region) {
      eventId
      vehicleId
      lat
      lon
      speedKph
      occurredAt
    }
  }
`;

export const ANOMALY_DETECTED_SUBSCRIPTION = gql`
  subscription AnomalyDetected($region: String) {
    anomalyDetected(region: $region) {
      id
      vehicleId
      region
      kind
      value
      detectorVersion
      windowStart
      windowEnd
    }
  }
`;
