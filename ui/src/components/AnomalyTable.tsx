import type { KeyboardEvent } from 'react';

import type { Anomaly } from '../types.js';

const MAX_VISIBLE_ANOMALIES = 200;

export interface AnomalyTableProps {
  anomalies: readonly Anomaly[];
  onSelectVehicle: (vehicleId: string) => void;
  loading?: boolean;
  error?: string | null;
}

function timestamp(value: string): number {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY;
}

function formatWindowEnd(value: string): string {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    return value;
  }

  return `${parsed.toISOString().slice(0, 19).replace('T', ' ')} UTC`;
}

function formatValue(value: number): string {
  if (!Number.isFinite(value)) {
    return '—';
  }

  return value.toFixed(2).replace(/\.?0+$/, '');
}

export function AnomalyTable({
  anomalies,
  onSelectVehicle,
  loading = false,
  error = null,
}: AnomalyTableProps) {
  const visibleAnomalies = anomalies
    .map((anomaly, inputIndex) => ({ anomaly, inputIndex }))
    .sort(
      (left, right) =>
        timestamp(right.anomaly.windowEnd) - timestamp(left.anomaly.windowEnd) ||
        left.inputIndex - right.inputIndex,
    )
    .slice(0, MAX_VISIBLE_ANOMALIES)
    .map(({ anomaly }) => anomaly);

  function selectVehicle(vehicleId: string, event?: KeyboardEvent<HTMLTableRowElement>): void {
    if (event && event.key !== 'Enter' && event.key !== ' ') {
      return;
    }

    event?.preventDefault();
    onSelectVehicle(vehicleId);
  }

  return (
    <section className="anomaly-panel" aria-labelledby="anomaly-table-title">
      <div className="panel-heading">
        <h2 id="anomaly-table-title">Anomalies</h2>
        {loading && visibleAnomalies.length > 0 ? (
          <span aria-live="polite" role="status">
            Updating…
          </span>
        ) : null}
      </div>

      {error ? <p role="alert">Couldn’t load anomalies: {error}</p> : null}

      <div
        className="anomaly-table-scroll"
        tabIndex={0}
        role="region"
        aria-label="Live anomaly table"
      >
        <table className="anomaly-table">
          <thead>
            <tr>
              <th scope="col">Time</th>
              <th scope="col">Vehicle</th>
              <th scope="col">Region</th>
              <th scope="col">Kind</th>
              <th scope="col">Value</th>
            </tr>
          </thead>
          <tbody>
            {visibleAnomalies.map((anomaly) => (
              <tr
                key={anomaly.id}
                className="anomaly-row"
                data-testid="anomaly-row"
                data-vehicle-id={anomaly.vehicleId}
                tabIndex={0}
                aria-label={`Show vehicle ${anomaly.vehicleId} on the map`}
                onClick={() => selectVehicle(anomaly.vehicleId)}
                onKeyDown={(event) => selectVehicle(anomaly.vehicleId, event)}
              >
                <td>
                  <time dateTime={anomaly.windowEnd}>{formatWindowEnd(anomaly.windowEnd)}</time>
                </td>
                <td>{anomaly.vehicleId}</td>
                <td>{anomaly.region}</td>
                <td>{anomaly.kind}</td>
                <td>{formatValue(anomaly.value)}</td>
              </tr>
            ))}

            {visibleAnomalies.length === 0 ? (
              <tr>
                <td colSpan={5}>
                  <span aria-live="polite" role="status">
                    {loading
                      ? 'Loading recent anomalies…'
                      : 'No anomalies detected in the last 15 minutes.'}
                  </span>
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
}
