import { makeExecutableSchema } from '@graphql-tools/schema';
import type { GraphQLSchema } from 'graphql';

import { createResolvers } from './resolvers.js';

export const typeDefs = `#graphql
scalar DateTime
enum VehicleStatus { ACTIVE IDLE OFFLINE }
enum AnomalyKind { SPEED_THRESHOLD SPEED_ZSCORE }

type Position { lat: Float!, lon: Float!, speedKph: Float!, headingDeg: Float!, updatedAt: DateTime! }
type VehicleEvent { eventId: ID!, vehicleId: ID!, lat: Float!, lon: Float!, speedKph: Float!, occurredAt: DateTime! }
type Anomaly { id: ID!, vehicleId: ID!, region: String!, kind: AnomalyKind!, value: Float!,
               detectorVersion: Int!, windowStart: DateTime!, windowEnd: DateTime! }
type RegionRollup { region: String!, windowStart: DateTime!, windowEnd: DateTime!,
                    eventCount: Int!, activeVehicles: Int!, avgSpeedKph: Float! }
type Vehicle {
  id: ID!, region: String!, status: VehicleStatus!, lastSeen: DateTime!
  position: Position!
  recentEvents(limit: Int = 20): [VehicleEvent!]!
  anomalies(since: DateTime): [Anomaly!]!
}
type Query {
  vehicles(region: String): [Vehicle!]!
  vehicle(id: ID!): Vehicle
  anomalies(region: String, since: DateTime): [Anomaly!]!
  regionRollups(region: String!, from: DateTime!, to: DateTime!): [RegionRollup!]!
}
type Subscription {
  vehicleMoved(region: String): VehicleEvent!
  anomalyDetected(region: String): Anomaly!
}
`;

export function createApiSchema(): GraphQLSchema {
  return makeExecutableSchema({
    typeDefs,
    resolvers: createResolvers(),
  });
}
