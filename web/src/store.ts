import type { Alert, Device, Summary } from './types';

export type DeviceMap = Map<string, Device>;

// Returns a new map only if something was newer than what we hold.
export function applyDevices(map: DeviceMap, items: Device[]): DeviceMap {
  let next: DeviceMap | null = null;
  for (const d of items) {
    const known = (next ?? map).get(d.deviceId);
    if (known && known.stateVersion >= d.stateVersion) continue;
    next ??= new Map(map);
    next.set(d.deviceId, d);
  }
  return next ?? map;
}

export function newerSummary(cur: Summary | null, s: Summary): Summary {
  return cur && cur.asOf > s.asOf ? cur : s;
}

export function sortedDevices(map: DeviceMap): Device[] {
  return [...map.values()].sort((a, b) => (a.deviceId < b.deviceId ? -1 : a.deviceId > b.deviceId ? 1 : 0));
}

// Snapshot is the source of truth: drops anything it doesn't list, reuses unchanged objects.
export function fromSnapshot(map: DeviceMap, items: Device[]): DeviceMap {
  return new Map(
    items.map((d) => {
      const known = map.get(d.deviceId);
      return [d.deviceId, known && known.stateVersion === d.stateVersion ? known : d];
    }),
  );
}

export type AlertMap = Map<number, Alert>;

export function applyAlerts(map: AlertMap, items: Alert[]): AlertMap {
  let next: AlertMap | null = null;
  for (const a of items) {
    const known = (next ?? map).get(a.alertId);
    if (known && known.version >= a.version) continue;
    next ??= new Map(map);
    next.set(a.alertId, a);
  }
  return next ?? map;
}

export function alertsFromSnapshot(map: AlertMap, items: Alert[]): AlertMap {
  return new Map(
    items.map((a) => {
      const known = map.get(a.alertId);
      return [a.alertId, known && known.version === a.version ? known : a];
    }),
  );
}

export const sortedAlerts = (map: AlertMap): Alert[] => [...map.values()].sort((a, b) => b.alertId - a.alertId);
