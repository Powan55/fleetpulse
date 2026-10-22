import { useCallback, useEffect, useRef, useState } from 'react';
import { STREAM_URL, fetchAlerts, fetchDevices, fetchSummary } from './api';
import { alertsFromSnapshot, applyAlerts, applyDevices, fromSnapshot, newerSummary, type AlertMap, type DeviceMap } from './store';
import type { Alert, Device, Summary } from './types';

type Kind = 'snapshot' | 'batch';
export type Conn = 'connecting' | 'connected' | 'disconnected';
export interface Fleet {
  devices: DeviceMap;
  summary: Summary | null;
  alerts: AlertMap;
  conn: Conn;
  ready: boolean;
  mark: Kind | null;
}

const RETRY_MS = 2000;
const initial: Fleet = { devices: new Map(), summary: null, alerts: new Map(), conn: 'connecting', ready: false, mark: null };

export function useFleet(): Fleet & { putAlert: (a: Alert) => void } {
  const [fleet, setFleet] = useState<Fleet>(initial);
  const put = useRef<(a: Alert) => void>(() => {});
  const putAlert = useCallback((a: Alert) => put.current(a), []);
  const { mark } = fleet;

  useEffect(() => {
    if (!mark) return;
    performance.mark(`fleetpulse.${mark}.end`);
    performance.measure(`fleetpulse.${mark}.render`, `fleetpulse.${mark}.start`, `fleetpulse.${mark}.end`);
  }, [mark, fleet]);

  useEffect(() => {
    let cur = initial;
    let pending: Kind | null = null;
    let frame = 0;
    let timer = 0;
    let es: EventSource | null = null;
    let gen = 0;
    let live = false;
    let buffer: [string, unknown][] = [];

    const schedule = () => {
      if (!frame) frame = requestAnimationFrame(flush);
    };
    const flush = () => {
      frame = 0;
      const kind = pending;
      pending = null;
      setFleet({ ...cur, mark: kind });
    };
    const touch = (kind: Kind) => {
      if (!pending || kind === 'snapshot') {
        performance.mark(`fleetpulse.${kind}.start`);
        pending = kind;
      }
      schedule();
    };
    const setConn = (conn: Conn) => {
      cur = { ...cur, conn };
      schedule();
    };

    const apply = (name: string, data: unknown) => {
      if (name === 'devices') {
        cur = { ...cur, devices: applyDevices(cur.devices, (data as { items: Device[] }).items) };
        touch('batch');
      } else if (name === 'alerts') {
        cur = { ...cur, alerts: applyAlerts(cur.alerts, (data as { items: Alert[] }).items) };
        touch('batch');
      } else if (name === 'fleet.summary') {
        cur = { ...cur, summary: newerSummary(cur.summary, data as Summary) };
        touch('batch');
      }
    };

    const snapshot = async (mine: number) => {
      live = false;
      buffer = [];
      try {
        const [page, summary, alerts] = await Promise.all([fetchDevices(), fetchSummary(), fetchAlerts()]);
        if (mine !== gen) return;
        cur = {
          ...cur,
          devices: fromSnapshot(cur.devices, page.items),
          summary,
          alerts: alertsFromSnapshot(cur.alerts, alerts.items),
          conn: 'connected',
          ready: true,
        };
        live = true;
        touch('snapshot');
        for (const [n, d] of buffer) apply(n, d);
        buffer = [];
      } catch {
        if (mine === gen) reconnect();
      }
    };

    const connect = () => {
      const mine = ++gen;
      live = false;
      const src = new EventSource(STREAM_URL);
      es = src;
      const on = (name: string, fn?: () => void) =>
        src.addEventListener(name, (ev) => {
          if (mine !== gen) return;
          if (fn) return fn();
          const data: unknown = JSON.parse((ev as MessageEvent<string>).data);
          if (live) apply(name, data);
          else buffer.push([name, data]);
        });
      on('hello', () => void snapshot(mine));
      on('resync', () => {
        setConn('connecting');
        void snapshot(mine);
      });
      on('devices');
      on('alerts');
      on('fleet.summary');
      src.onopen = () => {
        if (mine === gen && !live) setConn('connecting');
      };
      src.onerror = () => {
        if (mine !== gen) return;
        live = false;
        setConn('disconnected');
        if (src.readyState === EventSource.CLOSED) reconnect();
      };
    };

    const reconnect = () => {
      gen++;
      es?.close();
      setConn('disconnected');
      timer = window.setTimeout(connect, RETRY_MS);
    };

    put.current = (a) => {
      cur = { ...cur, alerts: applyAlerts(cur.alerts, [a]) };
      touch('batch');
    };
    connect();
    return () => {
      gen++;
      es?.close();
      cancelAnimationFrame(frame);
      clearTimeout(timer);
    };
  }, []);

  return { ...fleet, putAlert };
}
