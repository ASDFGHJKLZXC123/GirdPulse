/// <reference types="vite/client" />

import type { Map as MapLibreMap } from 'maplibre-gl';

declare global {
  interface ImportMetaEnv {
    readonly VITE_E2E?: string;
    readonly VITE_GRAPHQL_HTTP?: string;
    readonly VITE_GRAPHQL_WS?: string;
  }

  interface ImportMeta {
    readonly env: ImportMetaEnv;
  }

  interface Window {
    __map?: MapLibreMap;
  }
}

export {};
