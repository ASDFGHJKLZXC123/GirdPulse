import { useApolloClient, useQuery, useSubscription, type ApolloError } from '@apollo/client';
import { useEffect, useMemo, useRef, useState, type RefObject } from 'react';

import { AnomalyTable } from './components/AnomalyTable.js';
import { RollupStrip } from './components/RollupStrip.js';
import { VehicleMap, type VehicleMapHandle } from './components/VehicleMap.js';
import {
  ANOMALIES_QUERY,
  ANOMALY_DETECTED_SUBSCRIPTION,
  REGION_ROLLUPS_QUERY,
  VEHICLES_QUERY,
  VEHICLE_MOVED_SUBSCRIPTION,
} from './graphql.js';
import {
  anomalousVehicleIds,
  latestRollup,
  mergeAnomalies,
  mergeVehicleRefresh,
  patchVehicleMovements,
  regionVariable,
  rollingRange,
} from './state.js';
import {
  REGION_CODES,
  type AnomaliesQueryData,
  type AnomaliesQueryVariables,
  type Anomaly,
  type AnomalyDetectedSubscriptionData,
  type AnomalyDetectedSubscriptionVariables,
  type RegionCode,
  type RegionRollup,
  type RegionRollupsQueryData,
  type RegionRollupsQueryVariables,
  type RegionSelection,
  type Vehicle,
  type VehicleMovedSubscriptionData,
  type VehicleMovedSubscriptionVariables,
  type VehiclesQueryData,
  type VehiclesQueryVariables,
} from './types.js';

const THIRTY_SECONDS_MS = 30_000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1_000;
const REGION_OPTIONS = ['ALL', ...REGION_CODES] as const;

type RollupMap = Partial<Record<RegionCode, RegionRollup | null>>;
type RollupErrorMap = Partial<Record<RegionCode, string>>;

interface DashboardViewProps {
  anomalies: readonly Anomaly[];
  anomalyError?: string | null;
  anomalyLoading: boolean;
  anomalousIds: ReadonlySet<string>;
  liveError?: string | null;
  mapRef: RefObject<VehicleMapHandle | null>;
  onRegionChange: (region: RegionSelection) => void;
  rollupErrors: RollupErrorMap;
  rollupLoadingRegions: readonly RegionCode[];
  rollups: RollupMap;
  selectedRegion: RegionSelection;
  vehicleError?: string | null;
  vehicleLoading: boolean;
  vehicles: readonly Vehicle[];
}

function errorMessage(error: ApolloError | undefined): string | null {
  return error?.message ?? null;
}

export function DashboardView({
  anomalies,
  anomalyError = null,
  anomalyLoading,
  anomalousIds,
  liveError = null,
  mapRef,
  onRegionChange,
  rollupErrors,
  rollupLoadingRegions,
  rollups,
  selectedRegion,
  vehicleError = null,
  vehicleLoading,
  vehicles,
}: DashboardViewProps) {
  const operational = !vehicleError && !anomalyError && !liveError;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Real-time fleet intelligence</p>
          <h1>GridPulse Operations</h1>
          <p className="subtitle">
            Live positions, streaming anomalies, and five-minute regional health.
          </p>
        </div>

        <div className="topbar-controls">
          <span
            className={operational ? 'health-pill is-live' : 'health-pill is-degraded'}
            role="status"
          >
            <span aria-hidden="true" className="health-dot" />
            {operational ? 'Live pipeline' : 'Connection degraded'}
          </span>

          <label className="region-control" htmlFor="region-select">
            <span>Region</span>
            <select
              id="region-select"
              data-testid="region-select"
              value={selectedRegion}
              onChange={(event) => onRegionChange(event.currentTarget.value as RegionSelection)}
            >
              {REGION_OPTIONS.map((region) => (
                <option key={region} value={region}>
                  {region}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>

      <main>
        <section className="workspace" aria-label="Live fleet workspace">
          <div className="map-panel">
            <div className="panel-heading">
              <div>
                <p className="panel-kicker">Fleet map</p>
                <h2>{selectedRegion === 'ALL' ? 'All operating regions' : selectedRegion}</h2>
              </div>
              <span className="vehicle-count">
                {vehicleLoading && vehicles.length === 0
                  ? 'Loading vehicles…'
                  : `${vehicles.length} vehicles`}
              </span>
            </div>

            {vehicleError ? (
              <p className="inline-error" role="alert">
                Couldn’t refresh vehicles: {vehicleError}
              </p>
            ) : null}
            {liveError ? (
              <p className="inline-error" role="alert">
                Live connection: {liveError}
              </p>
            ) : null}

            <div className="map-frame">
              <VehicleMap
                ref={mapRef}
                anomalousVehicleIds={anomalousIds}
                selectedRegion={selectedRegion}
                vehicles={vehicles}
              />
              <div className="map-legend" aria-label="Vehicle status colors">
                <span>
                  <i className="legend-dot active" aria-hidden="true" />
                  Active
                </span>
                <span>
                  <i className="legend-dot idle" aria-hidden="true" />
                  Idle
                </span>
                <span>
                  <i className="legend-dot offline" aria-hidden="true" />
                  Offline
                </span>
                <span>
                  <i className="legend-dot anomalous" aria-hidden="true" />
                  Anomaly
                </span>
              </div>
            </div>
          </div>

          <div className="anomaly-column">
            <AnomalyTable
              anomalies={anomalies}
              loading={anomalyLoading}
              error={anomalyError}
              onSelectVehicle={(vehicleId) => mapRef.current?.flyToVehicle(vehicleId)}
            />
          </div>
        </section>

        <RollupStrip
          rollups={rollups}
          loadingRegions={rollupLoadingRegions}
          errors={rollupErrors}
        />
      </main>

      <footer>
        <span>GraphQL queries refresh state every 30 seconds.</span>
        <span>Movement and anomalies stream over WebSocket.</span>
      </footer>
    </div>
  );
}

function LiveDashboard({
  selectedRegion,
  onRegionChange,
}: {
  selectedRegion: RegionSelection;
  onRegionChange: (region: RegionSelection) => void;
}) {
  const client = useApolloClient();
  const mapRef = useRef<VehicleMapHandle>(null);
  const graphRegion = regionVariable(selectedRegion);
  const anomalySince = useMemo(
    () => new Date(Date.now() - FIFTEEN_MINUTES_MS).toISOString(),
    [selectedRegion],
  );
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [rollups, setRollups] = useState<RollupMap>({});
  const [rollupErrors, setRollupErrors] = useState<RollupErrorMap>({});
  const [rollupLoadingRegions, setRollupLoadingRegions] = useState<RegionCode[]>([...REGION_CODES]);
  const [movementError, setMovementError] = useState<string | null>(null);
  const [anomalyLiveError, setAnomalyLiveError] = useState<string | null>(null);
  const [clock, setClock] = useState(() => Date.now());
  const movementBufferRef = useRef(new Map<string, VehicleMovedSubscriptionData['vehicleMoved']>());
  const movementFrameRef = useRef<number | null>(null);
  const anomalyBufferRef = useRef(
    new Map<string, AnomalyDetectedSubscriptionData['anomalyDetected']>(),
  );
  const anomalyFrameRef = useRef<number | null>(null);

  const vehicleQuery = useQuery<VehiclesQueryData, VehiclesQueryVariables>(VEHICLES_QUERY, {
    variables: { region: graphRegion },
    fetchPolicy: 'network-only',
    pollInterval: THIRTY_SECONDS_MS,
  });
  const anomalyQuery = useQuery<AnomaliesQueryData, AnomaliesQueryVariables>(ANOMALIES_QUERY, {
    variables: {
      region: graphRegion,
      since: anomalySince,
    },
    fetchPolicy: 'network-only',
  });
  useSubscription<VehicleMovedSubscriptionData, VehicleMovedSubscriptionVariables>(
    VEHICLE_MOVED_SUBSCRIPTION,
    {
      variables: { region: graphRegion },
      ignoreResults: true,
      onData({ data }) {
        const event = data.data?.vehicleMoved;
        if (!event) return;

        const buffered = movementBufferRef.current.get(event.vehicleId);
        if (!buffered || Date.parse(event.occurredAt) >= Date.parse(buffered.occurredAt)) {
          movementBufferRef.current.set(event.vehicleId, event);
        }

        if (movementFrameRef.current === null) {
          movementFrameRef.current = window.requestAnimationFrame(() => {
            movementFrameRef.current = null;
            const events = [...movementBufferRef.current.values()];
            movementBufferRef.current.clear();
            setVehicles((current) => patchVehicleMovements(current, events));
            setMovementError(null);
          });
        }
      },
      onError(error) {
        setMovementError(error.message);
      },
    },
  );
  useSubscription<AnomalyDetectedSubscriptionData, AnomalyDetectedSubscriptionVariables>(
    ANOMALY_DETECTED_SUBSCRIPTION,
    {
      variables: { region: graphRegion },
      ignoreResults: true,
      onData({ data }) {
        const anomaly = data.data?.anomalyDetected;
        if (!anomaly) return;

        anomalyBufferRef.current.set(anomaly.id, anomaly);
        if (anomalyFrameRef.current === null) {
          anomalyFrameRef.current = window.requestAnimationFrame(() => {
            anomalyFrameRef.current = null;
            const incoming = [...anomalyBufferRef.current.values()];
            anomalyBufferRef.current.clear();
            setAnomalies((current) => mergeAnomalies(current, incoming));
            setAnomalyLiveError(null);
          });
        }
      },
      onError(error) {
        setAnomalyLiveError(error.message);
      },
    },
  );

  useEffect(() => {
    if (vehicleQuery.data) {
      const refreshedVehicles = vehicleQuery.data.vehicles;
      setVehicles((current) => mergeVehicleRefresh(current, refreshedVehicles));
    }
  }, [vehicleQuery.data]);

  useEffect(() => {
    if (anomalyQuery.data) {
      const seededAnomalies = anomalyQuery.data.anomalies;
      setAnomalies((current) => mergeAnomalies(current, seededAnomalies));
    }
  }, [anomalyQuery.data]);

  useEffect(() => {
    setMovementError(null);
    setAnomalyLiveError(null);

    return () => {
      if (movementFrameRef.current !== null) {
        window.cancelAnimationFrame(movementFrameRef.current);
        movementFrameRef.current = null;
      }
      if (anomalyFrameRef.current !== null) {
        window.cancelAnimationFrame(anomalyFrameRef.current);
        anomalyFrameRef.current = null;
      }
      movementBufferRef.current.clear();
      anomalyBufferRef.current.clear();
    };
  }, [graphRegion]);

  useEffect(() => {
    const timer = window.setInterval(() => setClock(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let disposed = false;
    let refreshing = false;

    const refreshRollups = async (): Promise<void> => {
      if (refreshing) return;
      refreshing = true;
      if (!disposed) setRollupLoadingRegions([...REGION_CODES]);

      const range = rollingRange();
      await Promise.all(
        REGION_CODES.map(async (region) => {
          try {
            const result = await client.query<RegionRollupsQueryData, RegionRollupsQueryVariables>({
              query: REGION_ROLLUPS_QUERY,
              variables: {
                region,
                ...range,
              },
              fetchPolicy: 'network-only',
            });

            if (disposed) return;
            setRollups((current) => ({
              ...current,
              [region]: latestRollup(result.data.regionRollups),
            }));
            setRollupErrors((current) => {
              const next = { ...current };
              delete next[region];
              return next;
            });
          } catch (error) {
            if (disposed) return;
            setRollupErrors((current) => ({
              ...current,
              [region]: error instanceof Error ? error.message : 'Unknown error',
            }));
          }
        }),
      );

      if (!disposed) {
        setRollupLoadingRegions([]);
      }
      refreshing = false;
    };

    void refreshRollups();
    const timer = window.setInterval(() => void refreshRollups(), THIRTY_SECONDS_MS);

    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [client]);

  const visibleVehicles = useMemo(
    () =>
      selectedRegion === 'ALL'
        ? vehicles
        : vehicles.filter((vehicle) => vehicle.region === selectedRegion),
    [selectedRegion, vehicles],
  );
  const visibleAnomalies = useMemo(
    () =>
      selectedRegion === 'ALL'
        ? anomalies
        : anomalies.filter((anomaly) => anomaly.region === selectedRegion),
    [anomalies, selectedRegion],
  );
  const anomalousIds = useMemo(
    () => anomalousVehicleIds(visibleAnomalies, clock),
    [clock, visibleAnomalies],
  );
  const liveError = movementError ?? anomalyLiveError;

  return (
    <DashboardView
      anomalies={visibleAnomalies}
      anomalyError={errorMessage(anomalyQuery.error)}
      anomalyLoading={anomalyQuery.loading}
      anomalousIds={anomalousIds}
      liveError={liveError}
      mapRef={mapRef}
      onRegionChange={onRegionChange}
      rollupErrors={rollupErrors}
      rollupLoadingRegions={rollupLoadingRegions}
      rollups={rollups}
      selectedRegion={selectedRegion}
      vehicleError={errorMessage(vehicleQuery.error)}
      vehicleLoading={vehicleQuery.loading}
      vehicles={visibleVehicles}
    />
  );
}

export function App() {
  const [selectedRegion, setSelectedRegion] = useState<RegionSelection>('ALL');

  return <LiveDashboard selectedRegion={selectedRegion} onRegionChange={setSelectedRegion} />;
}
