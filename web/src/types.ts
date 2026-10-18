export const STATES = ['HEALTHY', 'DEGRADED', 'FAILED', 'OFFLINE', 'RECOVERING'] as const;
export type DeviceState = (typeof STATES)[number];

export interface Device {
  deviceId: string;
  state: DeviceState;
  stateVersion: number;
  stateChangedAt: string;
  lastSeenAt: string;
}

export interface Summary {
  total: number;
  byState: Record<DeviceState, number>;
  asOf: string;
}

export interface DevicePage {
  items: Device[];
  page: number;
  size: number;
  totalItems: number;
}
