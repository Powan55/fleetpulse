import { useState } from 'react';
import { alertAction } from './api';
import type { Alert } from './types';

const fmt = (iso?: string | null) => (iso ? new Date(iso).toLocaleString() : '-');

export function AlertActions({ a, onChange }: { a: Alert; onChange: (a: Alert) => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const run = async (kind: 'ack' | 'resolve') => {
    setBusy(true);
    setError('');
    const r = await alertAction(a.alertId, kind);
    setBusy(false);
    if (r.ok) onChange(r.alert);
    else {
      setError(r.message);
      if (r.alert) onChange(r.alert);
    }
  };
  return (
    <>
      <button
        type="button"
        disabled={busy || a.status !== 'OPEN'}
        onClick={() => void run('ack')}
        data-testid={`alert-ack-${a.alertId}`}
      >
        Ack
      </button>{' '}
      <button
        type="button"
        disabled={busy || a.status === 'RESOLVED'}
        onClick={() => void run('resolve')}
        data-testid={`alert-resolve-${a.alertId}`}
      >
        Resolve
      </button>
      {error && (
        <span role="alert" className="s-FAILED" data-testid={`alert-error-${a.alertId}`}>
          {' '}
          {error}
        </span>
      )}
    </>
  );
}

export function AlertRow({ a, onChange, showDevice }: { a: Alert; onChange: (a: Alert) => void; showDevice: boolean }) {
  return (
    <tr data-testid={`alert-row-${a.alertId}`} data-status={a.status} data-device-id={a.deviceId}>
      <td>{a.alertId}</td>
      {showDevice && (
        <td>
          <a href={`#/devices/${a.deviceId}`}>{a.deviceId}</a>
        </td>
      )}
      <td>{a.ruleId}</td>
      <td className={`a-${a.status}`} data-testid={`alert-status-${a.alertId}`}>
        {a.status}
        {a.backfilled && <span data-testid={`alert-backfilled-${a.alertId}`}> (backfilled)</span>}
      </td>
      <td>{fmt(a.openedAt)}</td>
      <td>{a.status === 'RESOLVED' ? `${fmt(a.resolvedAt)} (${a.resolvedBy ?? '-'})` : '-'}</td>
      <td>
        <AlertActions a={a} onChange={onChange} />
      </td>
    </tr>
  );
}

export function AlertTable({
  alerts,
  onChange,
  showDevice,
  testid,
}: {
  alerts: Alert[];
  onChange: (a: Alert) => void;
  showDevice: boolean;
  testid: string;
}) {
  return (
    <div className="scroll" tabIndex={0} role="region" aria-label="Alerts">
      <table data-testid={testid}>
        <thead>
          <tr>
            <th scope="col">Id</th>
            {showDevice && <th scope="col">Device</th>}
            <th scope="col">Rule</th>
            <th scope="col">Status</th>
            <th scope="col">Opened</th>
            <th scope="col">Resolved</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {alerts.map((a) => (
            <AlertRow key={a.alertId} a={a} onChange={onChange} showDevice={showDevice} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function AlertsPage({ alerts, ready, onChange }: { alerts: Alert[]; ready: boolean; onChange: (a: Alert) => void }) {
  return (
    <section aria-label="Alerts" data-testid="alerts-page">
      <h2>Open alerts</h2>
      {!ready ? (
        <p data-testid="alerts-loading">Loading alerts...</p>
      ) : alerts.length === 0 ? (
        <p data-testid="alerts-empty">No open alerts.</p>
      ) : (
        <AlertTable alerts={alerts} onChange={onChange} showDevice testid="alert-list" />
      )}
    </section>
  );
}
