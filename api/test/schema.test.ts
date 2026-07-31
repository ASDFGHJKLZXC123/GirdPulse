import {
  GraphQLObjectType,
  GraphQLScalarType,
  buildSchema,
  introspectionFromSchema,
  type GraphQLNamedType,
} from 'graphql';
import { describe, expect, it } from 'vitest';

import { createApiSchema } from '../src/schema.js';

function assertObjectType(type: GraphQLNamedType | undefined | null): GraphQLObjectType {
  if (!(type instanceof GraphQLObjectType)) {
    throw new Error(`Expected an object type, received ${String(type)}`);
  }
  return type;
}

const EXACT_SDL = `
  scalar DateTime
  enum VehicleStatus { ACTIVE IDLE OFFLINE }
  enum AnomalyKind { SPEED_THRESHOLD SPEED_ZSCORE }

  type Position { lat: Float!, lon: Float!, speedKph: Float!, headingDeg: Float!, batteryPct: Float,
                  updatedAt: DateTime! }
  type VehicleEvent { eventId: ID!, vehicleId: ID!, lat: Float!, lon: Float!, speedKph: Float!, occurredAt: DateTime! }
  type Anomaly { id: ID!, vehicleId: ID!, region: String!, kind: AnomalyKind!, value: Float!,
                 detectorVersion: Int!, windowStart: DateTime!, windowEnd: DateTime! }
  type RegionRollup { region: String!, windowStart: DateTime!, windowEnd: DateTime!,
                      eventCount: Int!, activeVehicles: Int!, avgSpeedKph: Float! }
  type Vehicle {
    id: ID!, region: String!
    status: VehicleStatus! @deprecated(reason: "Will be replaced by a state model; see DEPRECATION.md")
    lastSeen: DateTime!
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

describe('M06 schema contract', () => {
  const introspectionOptions = {
    descriptions: false,
    schemaDescription: false,
    directiveIsRepeatable: true,
    specifiedByUrl: false,
    inputValueDeprecation: true,
    oneOf: false,
  } as const;

  it('matches the frozen SDL exactly through introspection', () => {
    const expected = introspectionFromSchema(buildSchema(EXACT_SDL), introspectionOptions);
    const actual = introspectionFromSchema(createApiSchema(), introspectionOptions);

    expect(actual).toEqual(expected);
  });

  it('adds batteryPct as a nullable Float and deprecates Vehicle.status without removing it', () => {
    const schema = createApiSchema();
    const position = assertObjectType(schema.getType('Position'));
    const vehicle = assertObjectType(schema.getType('Vehicle'));
    const batteryPct = position.getFields().batteryPct;
    const status = vehicle.getFields().status;

    expect(String(batteryPct.type)).toBe('Float');
    expect(batteryPct.deprecationReason).toBeFalsy();
    expect(String(status.type)).toBe('VehicleStatus!');
    expect(status.deprecationReason).toBe('Will be replaced by a state model; see DEPRECATION.md');
  });

  it('has no Mutation root and exposes only camelCase API fields', () => {
    const schema = createApiSchema();
    const objectFieldNames = [
      'Position',
      'VehicleEvent',
      'Anomaly',
      'RegionRollup',
      'Vehicle',
      'Query',
      'Subscription',
    ].flatMap((typeName) => {
      const type = schema.getType(typeName) as
        { getFields?: () => Record<string, unknown> } | undefined;
      return Object.keys(type?.getFields?.() ?? {});
    });

    expect(schema.getMutationType()).toBeUndefined();
    expect(objectFieldNames.join(' ')).not.toMatch(
      /speed_kph|heading_deg|updated_at|event_id|vehicle_id|occurred_at|detector_version|window_start|window_end|event_count|active_vehicles|avg_speed_kph/,
    );
  });

  it('serializes DateTime values as ISO-8601 strings', () => {
    const scalar = createApiSchema().getType('DateTime');
    expect(scalar).toBeInstanceOf(GraphQLScalarType);

    const dateTime = scalar as GraphQLScalarType;
    expect(JSON.stringify(dateTime.serialize(new Date('2026-07-24T12:34:56.789Z')))).toBe(
      '"2026-07-24T12:34:56.789Z"',
    );
    expect(JSON.stringify(dateTime.serialize('2026-07-24T12:34:56.789Z'))).toBe(
      '"2026-07-24T12:34:56.789Z"',
    );
    expect(() => dateTime.parseValue('not-a-date')).toThrow();
  });
});
