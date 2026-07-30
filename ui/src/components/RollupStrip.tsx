import type { RegionCode, RegionRollup } from '../types.js';

const REGIONS = ['SEA', 'SFO', 'NYC', 'AUS'] as const satisfies readonly RegionCode[];

export interface RollupStripProps {
  rollups: Partial<Record<RegionCode, RegionRollup | null>>;
  loadingRegions?: readonly RegionCode[];
  errors?: Partial<Record<RegionCode, string>>;
}

function formatAverageSpeed(value: number): string {
  return Number.isFinite(value) ? `${value.toFixed(1)} km/h` : '—';
}

export function RollupStrip({ rollups, loadingRegions = [], errors = {} }: RollupStripProps) {
  const loading = new Set<RegionCode>(loadingRegions);

  return (
    <section className="rollup-strip" aria-labelledby="rollup-strip-title">
      <h2 id="rollup-strip-title">Regional activity</h2>
      <ul className="rollup-grid" aria-label="Latest five-minute regional rollups">
        {REGIONS.map((region) => {
          const rollup = rollups[region];
          const isLoading = loading.has(region);
          const error = errors[region];
          const labelId = `rollup-${region}-label`;

          return (
            <li
              key={region}
              className="rollup-cell"
              data-testid={`rollup-${region}`}
              aria-labelledby={labelId}
              aria-busy={isLoading}
            >
              <h3 id={labelId}>{region}</h3>

              {rollup ? (
                <>
                  <dl className="rollup-metrics">
                    <div>
                      <dt>Events</dt>
                      <dd>{rollup.eventCount}</dd>
                    </div>
                    <div>
                      <dt>Active vehicles</dt>
                      <dd>{rollup.activeVehicles}</dd>
                    </div>
                    <div>
                      <dt>Average speed</dt>
                      <dd>{formatAverageSpeed(rollup.avgSpeedKph)}</dd>
                    </div>
                  </dl>

                  {isLoading ? (
                    <p aria-live="polite" role="status">
                      Refreshing…
                    </p>
                  ) : null}
                  {error ? <p role="alert">Refresh failed: {error}</p> : null}
                </>
              ) : (
                <p aria-live="polite" role={error ? 'alert' : 'status'}>
                  {error
                    ? `Rollup unavailable: ${error}`
                    : isLoading
                      ? 'Loading latest rollup…'
                      : 'No recent rollup data.'}
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
