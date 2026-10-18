import type { DevicePage, Summary } from './types';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return (await res.json()) as T;
}

export const STREAM_URL = '/api/v1/stream';
export const fetchDevices = () => get<DevicePage>('/api/v1/devices?size=1000');
export const fetchSummary = () => get<Summary>('/api/v1/fleet/summary');
