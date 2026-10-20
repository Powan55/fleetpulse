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

export interface TelemetryPoint {
  epoch: number;
  seq: number;
  eventTime: string;
  receivedAt: string;
  temperatureC: number;
  batteryPct: number;
  errorCount: number;
  replay: boolean;
}

export interface StateTransition {
  id: number;
  fromState: DeviceState;
  toState: DeviceState;
  cause: string;
  eventId?: string | null;
  eventTime?: string | null;
  occurredAt: string;
  replay: boolean;
}

export interface DeviceDetail extends Device {
  lastEventTime?: string | null;
  lastEpoch: number;
  lastSeq: number;
  recentTelemetry: TelemetryPoint[];
  stateHistory: StateTransition[];
  alerts: unknown[];
}

export type DetailResult =
  | { ok: true; detail: DeviceDetail }
  | { ok: false; status: number; message: string };
