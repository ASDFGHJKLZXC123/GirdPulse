import type { AnomalyDetectedPayload, VehicleMovedPayload } from './event-mappers.js';

export const VEHICLE_MOVED_TOPIC = 'VEHICLE_MOVED';
export const ANOMALY_DETECTED_TOPIC = 'ANOMALY_DETECTED';

export interface EventPayloads {
  VEHICLE_MOVED: VehicleMovedPayload;
  ANOMALY_DETECTED: AnomalyDetectedPayload;
}

export type EventTopic = keyof EventPayloads;

interface PendingNext<T> {
  resolve: (result: IteratorResult<T>) => void;
  reject: (error: unknown) => void;
}

class EventSubscription<T> implements AsyncIterableIterator<T> {
  private readonly queued: T[] = [];
  private readonly pending: PendingNext<T>[] = [];
  private closed = false;

  constructor(private readonly unsubscribe: () => void) {}

  push(value: T): void {
    if (this.closed) {
      return;
    }

    const next = this.pending.shift();
    if (next) {
      next.resolve({ value, done: false });
      return;
    }
    this.queued.push(value);
  }

  next(): Promise<IteratorResult<T>> {
    const value = this.queued.shift();
    if (value !== undefined) {
      return Promise.resolve({ value, done: false });
    }
    if (this.closed) {
      return Promise.resolve({ value: undefined, done: true });
    }

    return new Promise<IteratorResult<T>>((resolve, reject) => {
      this.pending.push({ resolve, reject });
    });
  }

  return(): Promise<IteratorResult<T>> {
    this.finish(false);
    return Promise.resolve({ value: undefined, done: true });
  }

  throw(error?: unknown): Promise<IteratorResult<T>> {
    this.finish(true, error);
    return Promise.reject(error);
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }

  private finish(failed: boolean, error?: unknown): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.queued.length = 0;
    this.unsubscribe();
    for (const next of this.pending.splice(0)) {
      if (failed) {
        next.reject(error);
      } else {
        next.resolve({ value: undefined, done: true });
      }
    }
  }
}

export class EventBus {
  private readonly subscriptions = new Map<EventTopic, Set<unknown>>();

  publish<K extends EventTopic>(topic: K, payload: EventPayloads[K]): void {
    const subscriptions = this.subscriptions.get(topic);
    if (!subscriptions) {
      return;
    }

    for (const subscription of subscriptions) {
      (subscription as EventSubscription<EventPayloads[K]>).push(payload);
    }
  }

  subscribe<K extends EventTopic>(topic: K): AsyncIterableIterator<EventPayloads[K]> {
    let subscriptions = this.subscriptions.get(topic);
    if (!subscriptions) {
      subscriptions = new Set<unknown>();
      this.subscriptions.set(topic, subscriptions);
    }

    const subscription = new EventSubscription<EventPayloads[K]>(() => {
      subscriptions.delete(subscription);
      if (subscriptions.size === 0) {
        this.subscriptions.delete(topic);
      }
    });
    subscriptions.add(subscription);
    return subscription;
  }
}
