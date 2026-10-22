import type { ActionResult, Alert, AlertPage, DetailResult, DeviceDetail, DevicePage, Summary } from './types';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return (await res.json()) as T;
}

export const STREAM_URL = '/api/v1/stream';
export const fetchDevices = () => get<DevicePage>('/api/v1/devices?size=1000');
export const fetchAlerts = () => get<AlertPage>('/api/v1/alerts?status=OPEN,ACKED&size=200');
export const fetchSummary = () => get<Summary>('/api/v1/fleet/summary');

export async function fetchDevice(deviceId: string): Promise<DetailResult> {
  try {
    const res = await fetch(`/api/v1/devices/${encodeURIComponent(deviceId)}`, {
      headers: { Accept: 'application/json' },
    });
    if (res.ok) return { ok: true, detail: (await res.json()) as DeviceDetail };
    let message = `Request failed (${res.status})`;
    try {
      const p = (await res.json()) as { title?: string; detail?: string };
      message = p.detail ?? p.title ?? message;
    } catch {
      /* non-JSON error body */
    }
    return { ok: false, status: res.status, message };
  } catch {
    return { ok: false, status: 0, message: 'Cannot reach the api.' };
  }
}

export async function alertAction(alertId: number, kind: 'ack' | 'resolve'): Promise<ActionResult> {
  try {
    const res = await fetch(`/api/v1/alerts/${alertId}/${kind}`, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
      body: '{}',
    });
    if (res.ok) return { ok: true, alert: (await res.json()) as Alert };
    let message = `Request failed (${res.status})`;
    let alert: Alert | undefined;
    try {
      const p = (await res.json()) as { title?: string; detail?: string; alert?: Alert };
      message = p.detail ?? p.title ?? message;
      alert = p.alert;
    } catch {
      /* non-JSON error body */
    }
    if (res.status === 409) message = 'Alert is already resolved.';
    return { ok: false, status: res.status, message, alert };
  } catch {
    return { ok: false, status: 0, message: 'Cannot reach the api.' };
  }
}
