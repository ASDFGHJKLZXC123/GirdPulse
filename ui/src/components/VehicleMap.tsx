import {
  GeoJSONSource,
  Map as MapLibreMap,
  Popup,
  setWorkerUrl,
  type StyleSpecification,
} from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import maplibreWorkerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';
import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef } from 'react';

import { MAP_STYLE } from '../map-style.js';
import type { RegionSelection, Vehicle } from '../types.js';
import { createPopupContent } from './vehicle-popup.js';

const SOURCE_ID = 'vehicles';
const STATUS_LAYER_ID = 'vehicle-status';
const ANOMALY_LAYER_ID = 'vehicle-anomaly-pulse';
const PULSE_DURATION_MS = 1_200;

setWorkerUrl(maplibreWorkerUrl);

const REGION_VIEWS: Record<RegionSelection, { center: [number, number]; zoom: number }> = {
  ALL: { center: [-98.5, 39], zoom: 3.2 },
  SEA: { center: [-122.34, 47.6], zoom: 10 },
  SFO: { center: [-122.435, 37.76], zoom: 10 },
  NYC: { center: [-73.95, 40.78], zoom: 10 },
  AUS: { center: [-97.75, 30.3], zoom: 10 },
};

interface VehicleFeatureCollection {
  type: 'FeatureCollection';
  features: Array<{
    type: 'Feature';
    id: string;
    geometry: {
      type: 'Point';
      coordinates: [number, number];
    };
    properties: {
      anomalous: boolean;
      id: string;
      lastSeen: string;
      region: string;
      speedKph: number;
      status: Vehicle['status'];
    };
  }>;
}

declare global {
  interface Window {
    __map?: MapLibreMap;
  }
}

export interface VehicleMapHandle {
  flyToVehicle: (vehicleId: string) => void;
}

export interface VehicleMapProps {
  anomalousVehicleIds: ReadonlySet<string>;
  selectedRegion: RegionSelection;
  vehicles: readonly Vehicle[];
}

function toFeatureCollection(
  vehicles: readonly Vehicle[],
  anomalousVehicleIds: ReadonlySet<string>,
): VehicleFeatureCollection {
  return {
    type: 'FeatureCollection',
    features: vehicles.map((vehicle) => ({
      type: 'Feature',
      id: vehicle.id,
      geometry: {
        type: 'Point',
        coordinates: [vehicle.position.lon, vehicle.position.lat],
      },
      properties: {
        anomalous: anomalousVehicleIds.has(vehicle.id),
        id: vehicle.id,
        lastSeen: vehicle.lastSeen,
        region: vehicle.region,
        speedKph: vehicle.position.speedKph,
        status: vehicle.status,
      },
    })),
  };
}

export const VehicleMap = forwardRef<VehicleMapHandle, VehicleMapProps>(function VehicleMap(
  { anomalousVehicleIds, selectedRegion, vehicles },
  ref,
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const popupRef = useRef<Popup | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const vehiclesRef = useRef(vehicles);

  vehiclesRef.current = vehicles;

  const featureCollection = useMemo(
    () => toFeatureCollection(vehicles, anomalousVehicleIds),
    [anomalousVehicleIds, vehicles],
  );
  const featureCollectionRef = useRef(featureCollection);
  featureCollectionRef.current = featureCollection;

  useImperativeHandle(
    ref,
    () => ({
      flyToVehicle(vehicleId: string) {
        const map = mapRef.current;
        const vehicle = vehiclesRef.current.find((candidate) => candidate.id === vehicleId);

        if (!map || !vehicle) return;

        const coordinates: [number, number] = [vehicle.position.lon, vehicle.position.lat];

        map.flyTo({
          center: coordinates,
          essential: true,
          zoom: Math.max(map.getZoom(), 12),
        });

        popupRef.current?.remove();
        popupRef.current = new Popup({ closeButton: true })
          .setLngLat(coordinates)
          .setDOMContent(createPopupContent(vehicle))
          .addTo(map);
      },
    }),
    [],
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const initialView = REGION_VIEWS[selectedRegion];
    const map = new MapLibreMap({
      container,
      center: initialView.center,
      style: MAP_STYLE as StyleSpecification | string,
      zoom: initialView.zoom,
    });
    mapRef.current = map;

    if (import.meta.env.VITE_E2E === 'true') window.__map = map;

    const addVehicleSourceAndLayers = () => {
      if (!map.getSource(SOURCE_ID)) {
        map.addSource(SOURCE_ID, {
          type: 'geojson',
          data: featureCollectionRef.current,
        });
      }

      if (!map.getLayer(STATUS_LAYER_ID)) {
        map.addLayer({
          id: STATUS_LAYER_ID,
          type: 'circle',
          source: SOURCE_ID,
          paint: {
            'circle-color': [
              'match',
              ['get', 'status'],
              'ACTIVE',
              '#16a34a',
              'IDLE',
              '#6b7280',
              'OFFLINE',
              '#dc2626',
              '#6b7280',
            ],
            'circle-opacity': 0.95,
            'circle-radius': 6,
            'circle-stroke-color': '#ffffff',
            'circle-stroke-width': 1.5,
          },
        });
      }

      if (!map.getLayer(ANOMALY_LAYER_ID)) {
        map.addLayer({
          id: ANOMALY_LAYER_ID,
          type: 'circle',
          source: SOURCE_ID,
          filter: ['==', ['get', 'anomalous'], true],
          paint: {
            'circle-color': '#f97316',
            'circle-opacity': 0.8,
            'circle-radius': 8,
            'circle-stroke-color': '#fb923c',
            'circle-stroke-opacity': 0.9,
            'circle-stroke-width': 2,
          },
        });
      }

      const animatePulse = (timestamp: number) => {
        const progress = (timestamp % PULSE_DURATION_MS) / PULSE_DURATION_MS;
        const wave = (Math.sin(progress * Math.PI * 2) + 1) / 2;

        if (map.getLayer(ANOMALY_LAYER_ID)) {
          map.setPaintProperty(ANOMALY_LAYER_ID, 'circle-radius', 8 + wave * 7);
          map.setPaintProperty(ANOMALY_LAYER_ID, 'circle-opacity', 0.3 + (1 - wave) * 0.55);
          map.setPaintProperty(ANOMALY_LAYER_ID, 'circle-stroke-opacity', 0.35 + (1 - wave) * 0.55);
        }

        animationFrameRef.current = window.requestAnimationFrame(animatePulse);
      };

      animationFrameRef.current = window.requestAnimationFrame(animatePulse);
    };

    map.on('load', addVehicleSourceAndLayers);

    return () => {
      map.off('load', addVehicleSourceAndLayers);

      if (animationFrameRef.current !== null) {
        window.cancelAnimationFrame(animationFrameRef.current);
        animationFrameRef.current = null;
      }

      popupRef.current?.remove();
      popupRef.current = null;

      if (import.meta.env.VITE_E2E === 'true' && window.__map === map) {
        delete window.__map;
      }

      map.remove();
      if (mapRef.current === map) mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const source = mapRef.current?.getSource(SOURCE_ID);
    if (source instanceof GeoJSONSource) {
      void source.setData(featureCollection);
    }
  }, [featureCollection]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    popupRef.current?.remove();
    popupRef.current = null;

    const view = REGION_VIEWS[selectedRegion];
    map.flyTo({
      center: view.center,
      essential: true,
      zoom: view.zoom,
    });
  }, [selectedRegion]);

  return (
    <>
      <div ref={containerRef} className="map-canvas" data-testid="map-canvas" />
      <ul aria-hidden="true" hidden>
        {vehicles.map((vehicle) => (
          <li
            key={vehicle.id}
            data-testid={`vehicle-marker-${vehicle.id}`}
            data-lat={vehicle.position.lat}
            data-lon={vehicle.position.lon}
            data-region={vehicle.region}
          />
        ))}
      </ul>
    </>
  );
});
