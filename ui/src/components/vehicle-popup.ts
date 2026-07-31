import type { Vehicle } from '../types.js';

export function formatBatteryPct(batteryPct: number | null): string {
  return batteryPct === null ? 'Unknown' : `${batteryPct.toFixed(1)}%`;
}

export function createPopupContent(vehicle: Vehicle): HTMLDivElement {
  const container = document.createElement('div');
  container.className = 'vehicle-popup';

  const title = document.createElement('strong');
  title.textContent = vehicle.id;
  container.append(title);

  const details = [
    ['Speed', `${vehicle.position.speedKph.toFixed(1)} km/h`],
    ['Battery', formatBatteryPct(vehicle.position.batteryPct)],
    ['Status', vehicle.status],
    ['Last seen', vehicle.lastSeen],
  ] as const;

  for (const [label, value] of details) {
    const row = document.createElement('div');
    const labelNode = document.createElement('span');
    const valueNode = document.createElement('span');

    labelNode.textContent = `${label}: `;
    valueNode.textContent = value;
    row.append(labelNode, valueNode);
    container.append(row);
  }

  return container;
}
