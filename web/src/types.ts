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
  alerts: Alert[];
}

export type DetailResult =
  | { ok: true; detail: DeviceDetail }
  | { ok: false; status: number; message: string };

export type AlertStatus = 'OPEN' | 'ACKED' | 'RESOLVED';

export interface Alert {
  alertId: number;
  deviceId: string;
  ruleId: string;
  status: AlertStatus;
  version: number;
  backfilled: boolean;
  openedAt: string;
  ackedAt?: string | null;
  resolvedAt?: string | null;
  resolvedBy?: 'auto' | 'operator' | null;
  updatedAt: string;
}

export interface AlertPage {
  items: Alert[];
  page: number;
  size: number;
  totalItems: number;
}

export type ActionResult =
  | { ok: true; alert: Alert }
  | { ok: false; status: number; message: string; alert?: Alert };
