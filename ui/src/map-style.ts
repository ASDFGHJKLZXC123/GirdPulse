import type { StyleSpecification } from 'maplibre-gl';

export const NORMAL_MAP_STYLE_URL = 'https://demotiles.maplibre.org/style.json';

export const E2E_MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {},
  layers: [
    {
      id: 'background',
      type: 'background',
      paint: {
        'background-color': '#eef2f7',
      },
    },
  ],
};

export const MAP_STYLE: string | StyleSpecification =
  import.meta.env.VITE_E2E === 'true' ? E2E_MAP_STYLE : NORMAL_MAP_STYLE_URL;
