import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { applyDevices, fromSnapshot, newerSummary, sortedDevices } from './store';
import { Tile, App } from './App';
import { DetailView } from './Detail';
import type { Device, Summary } from './types';

const dev = (deviceId: string, stateVersion: number, state: Device['state'] = 'HEALTHY'): Device => ({
  deviceId,
  state,
  stateVersion,
  stateChangedAt: '2026-01-01T00:00:00Z',
  lastSeenAt: '2026-01-01T00:00:00Z',
});

describe('applyDevices', () => {
  it('adds new devices', () => {
    const m = applyDevices(new Map(), [dev('dev-0002', 1), dev('dev-0001', 1)]);
    expect(sortedDevices(m).map((d) => d.deviceId)).toEqual(['dev-0001', 'dev-0002']);
  });

  it('drops stale and equal versions, keeps the same map', () => {
    const m = applyDevices(new Map(), [dev('dev-0001', 3, 'FAILED')]);
    expect(applyDevices(m, [dev('dev-0001', 3, 'HEALTHY'), dev('dev-0001', 2, 'HEALTHY')])).toBe(m);
    expect(m.get('dev-0001')?.state).toBe('FAILED');
  });

  it('applies newer versions, untouched entries keep identity', () => {
    const m = applyDevices(new Map(), [dev('dev-0001', 1), dev('dev-0002', 1)]);
    const n = applyDevices(m, [dev('dev-0001', 2, 'OFFLINE')]);
    expect(n.get('dev-0001')?.state).toBe('OFFLINE');
    expect(n.get('dev-0002')).toBe(m.get('dev-0002'));
  });
});

describe('fromSnapshot', () => {
  it('replaces state even if versions went backwards, reuses equal entries', () => {
    const m = applyDevices(new Map(), [dev('dev-0001', 9, 'FAILED'), dev('dev-0002', 1)]);
    const n = fromSnapshot(m, [dev('dev-0001', 1), dev('dev-0002', 1)]);
    expect(n.get('dev-0001')?.state).toBe('HEALTHY');
    expect(n.get('dev-0002')).toBe(m.get('dev-0002'));
  });
});

describe('newerSummary', () => {
  const s = (asOf: string): Summary => ({
    total: 1,
    byState: { HEALTHY: 1, DEGRADED: 0, FAILED: 0, OFFLINE: 0, RECOVERING: 0 },
    asOf,
  });
  it('keeps the later asOf', () => {
    expect(newerSummary(s('2026-01-02T00:00:00Z'), s('2026-01-01T00:00:00Z')).asOf).toBe('2026-01-02T00:00:00Z');
    expect(newerSummary(null, s('2026-01-01T00:00:00Z')).asOf).toBe('2026-01-01T00:00:00Z');
  });
});

describe('markup', () => {
  it('tile has testid and readable state text', () => {
    const html = renderToStaticMarkup(<Tile d={dev('dev-0007', 1, 'DEGRADED')} />);
    expect(html).toContain('data-testid="device-tile-dev-0007"');
    expect(html).toContain('>DEGRADED<');
  });

  it('app shell has indicator, filter, root not ready', () => {
    const html = renderToStaticMarkup(<App />);
    expect(html).toContain('data-testid="connection-indicator"');
    expect(html).toContain('data-testid="state-filter"');
    expect(html).toContain('data-ready="false"');
  });

  it('tile links to the detail route', () => {
    expect(renderToStaticMarkup(<Tile d={dev('dev-0007', 1)} />)).toContain('href="#/devices/dev-0007"');
  });

  it('detail shows 404 as a message', () => {
    const html = renderToStaticMarkup(
      <DetailView id="dev-0999" result={{ ok: false, status: 404, message: 'x' }} onRetry={() => {}} />,
    );
    expect(html).toContain('data-testid="detail-not-found"');
    expect(html).toContain('dev-0999 not found');
  });

  it('detail renders fields, telemetry and history', () => {
    const detail = {
      ...dev('dev-0001', 2, 'FAILED'),
      lastEpoch: 3,
      lastSeq: 42,
      recentTelemetry: [
        {
          epoch: 3,
          seq: 42,
          eventTime: '',
          receivedAt: '2026-01-01T00:00:00Z',
          temperatureC: 21.5,
          batteryPct: 80,
          errorCount: 0,
          replay: false,
        },
      ],
      stateHistory: [
        {
          id: 1,
          fromState: 'HEALTHY' as const,
          toState: 'FAILED' as const,
          cause: 'telemetry',
          occurredAt: '2026-01-01T00:00:00Z',
          replay: false,
        },
      ],
      alerts: [],
    };
    const html = renderToStaticMarkup(<DetailView id="dev-0001" result={{ ok: true, detail }} onRetry={() => {}} />);
    for (const t of ['detail-state', 'detail-last-seq', 'telemetry-row', 'history-row'])
      expect(html).toContain(`data-testid="${t}"`);
    expect(html).toContain('>42<');
  });
});
