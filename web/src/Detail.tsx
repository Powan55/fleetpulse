import { useEffect, useState } from 'react';
import { fetchDevice } from './api';
import { AlertTable } from './Alerts';
import type { Alert, DetailResult, Device, TelemetryPoint } from './types';

const fmt = (iso?: string | null) => (iso ? new Date(iso).toLocaleString() : '-');

const W = 600;
const H = 120;
function line(vals: number[]) {
  const lo = Math.min(...vals);
  const span = Math.max(...vals) - lo || 1;
  return vals.map((v, i) => `${(i / Math.max(vals.length - 1, 1)) * W},${H - ((v - lo) / span) * H}`).join(' ');
}
const range = (v: number[]) => `${Math.min(...v)} to ${Math.max(...v)}`;

export function Chart({ points }: { points: TelemetryPoint[] }) {
  const p = [...points].reverse();
  const temp = p.map((t) => t.temperatureC);
  const batt = p.map((t) => t.batteryPct);
  return (
    <figure className="chart" data-testid="telemetry-chart">
      <svg
        viewBox={`0 0 ${W} ${H}`}
        role="img"
        aria-label={`Temperature and battery, last ${p.length} readings, oldest left`}
        preserveAspectRatio="none"
      >
        <polyline points={line(temp)} className="line-temp" />
        <polyline points={line(batt)} className="line-batt" />
      </svg>
      <figcaption>
        <span className="line-temp-key">Temperature {range(temp)} C</span>
        {' | '}
        <span className="line-batt-key">Battery {range(batt)} %</span>
      </figcaption>
    </figure>
  );
}

export function DetailView({
  id,
  live,
  result,
  onRetry,
  alerts,
  onAlert = () => {},
}: {
  id: string;
  live?: Device;
  result: DetailResult | null;
  onRetry: () => void;
  alerts?: Map<number, Alert>;
  onAlert?: (a: Alert) => void;
}) {
  const merged: Alert[] = result?.ok
    ? [
        ...new Map(
          [...result.detail.alerts, ...[...(alerts?.values() ?? [])].filter((a) => a.deviceId === id)]
            .sort((x, y) => x.version - y.version)
            .map((a) => [a.alertId, a] as const),
        ).values(),
      ].sort((x, y) => y.alertId - x.alertId)
    : [];
  return (
    <section aria-label={`Device ${id}`} data-testid="device-detail" data-device-id={id}>
      <p>
        <a href="#/" data-testid="detail-back">
          Back to fleet
        </a>
      </p>
      <h2>{id}</h2>
      {!result ? (
        <p data-testid="detail-loading">Loading device...</p>
      ) : !result.ok ? (
        <div role="alert" data-testid={result.status === 404 ? 'detail-not-found' : 'detail-error'}>
          <p>{result.status === 404 ? `Device ${id} not found.` : result.message}</p>
          {result.status !== 404 && result.status !== 400 && (
            <button type="button" onClick={onRetry} data-testid="detail-retry">
              Retry
            </button>
          )}
        </div>
      ) : (
        <>
          <dl className="fields">
            <dt>State</dt>
            <dd data-testid="detail-state" className={`s-${(live ?? result.detail).state}`}>
              {(live ?? result.detail).state}
            </dd>
            <dt>State version</dt>
            <dd data-testid="detail-state-version">{result.detail.stateVersion}</dd>
            <dt>State changed</dt>
            <dd>{fmt(result.detail.stateChangedAt)}</dd>
            <dt>Last seen</dt>
            <dd data-testid="detail-last-seen">{fmt(result.detail.lastSeenAt)}</dd>
            <dt>Last epoch</dt>
            <dd data-testid="detail-last-epoch">{result.detail.lastEpoch}</dd>
            <dt>Last seq</dt>
            <dd data-testid="detail-last-seq">{result.detail.lastSeq}</dd>
          </dl>

          <h3>Alerts</h3>
          {merged.length === 0 ? (
            <p data-testid="detail-alerts-empty">No alerts for this device.</p>
          ) : (
            <AlertTable alerts={merged} onChange={onAlert} showDevice={false} testid="detail-alerts" />
          )}

          <h3>Telemetry chart</h3>
          {result.detail.recentTelemetry.length === 0 ? (
            <p data-testid="chart-empty">No telemetry to chart.</p>
          ) : (
            <Chart points={result.detail.recentTelemetry} />
          )}

          <h3>Recent telemetry</h3>
          {result.detail.recentTelemetry.length === 0 ? (
            <p data-testid="telemetry-empty">No telemetry in the retention window.</p>
          ) : (
            <div className="scroll" tabIndex={0} role="region" aria-label="Recent telemetry">
              <table data-testid="telemetry-table">
                <thead>
                  <tr>
                    <th scope="col">Epoch</th>
                    <th scope="col">Seq</th>
                    <th scope="col">Received</th>
                    <th scope="col">Temp C</th>
                    <th scope="col">Battery %</th>
                    <th scope="col">Errors</th>
                    <th scope="col">Replay</th>
                  </tr>
                </thead>
                <tbody>
                  {result.detail.recentTelemetry.map((t) => (
                    <tr key={`${t.epoch}-${t.seq}`} data-testid="telemetry-row">
                      <td>{t.epoch}</td>
                      <td>{t.seq}</td>
                      <td>{fmt(t.receivedAt)}</td>
                      <td>{t.temperatureC}</td>
                      <td>{t.batteryPct}</td>
                      <td>{t.errorCount}</td>
                      <td>{t.replay ? 'yes' : 'no'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <h3>State timeline</h3>
          {result.detail.stateHistory.length === 0 ? (
            <p data-testid="history-empty">No state changes recorded.</p>
          ) : (
            <ol className="history" data-testid="history-list">
              {result.detail.stateHistory.map((h) => (
                <li key={h.id} data-testid="history-row">
                  {fmt(h.occurredAt)}: <span className={`s-${h.fromState}`}>{h.fromState}</span> to{' '}
                  <span className={`s-${h.toState}`}>{h.toState}</span> ({h.cause}
                  {h.replay ? ', replay' : ''})
                </li>
              ))}
            </ol>
          )}
        </>
      )}
    </section>
  );
}

interface Loaded {
  id: string;
  version: number | undefined;
  result: DetailResult;
}

export function Detail({
  id,
  live,
  alerts,
  onAlert,
}: {
  id: string;
  live?: Device;
  alerts: Map<number, Alert>;
  onAlert: (a: Alert) => void;
}) {
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [retry, setRetry] = useState(0);
  const version = live?.stateVersion;

  useEffect(() => {
    let stale = false;
    void fetchDevice(id).then((result) => {
      if (!stale) setLoaded({ id, version, result });
    });
    return () => {
      stale = true;
    };
  }, [id, version, retry]);

  const result = loaded && loaded.id === id ? loaded.result : null;
  return (
    <DetailView
      id={id}
      live={live}
      result={result}
      onRetry={() => setRetry((n) => n + 1)}
      alerts={alerts}
      onAlert={onAlert}
    />
  );
}
