import type { DetailResult, DeviceDetail, DevicePage, Summary } from './types';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return (await res.json()) as T;
}

export const STREAM_URL = '/api/v1/stream';
export const fetchDevices = () => get<DevicePage>('/api/v1/devices?size=1000');
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
