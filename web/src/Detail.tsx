import { useEffect, useState } from 'react';
import { fetchDevice } from './api';
import type { DetailResult, Device } from './types';

const fmt = (iso?: string | null) => (iso ? new Date(iso).toLocaleString() : '-');

export function DetailView({
  id,
  live,
  result,
  onRetry,
}: {
  id: string;
  live?: Device;
  result: DetailResult | null;
  onRetry: () => void;
}) {
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

          <h3>State history</h3>
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

export function Detail({ id, live }: { id: string; live?: Device }) {
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
  return <DetailView id={id} live={live} result={result} onRetry={() => setRetry((n) => n + 1)} />;
}
