import { memo, useMemo, useState } from 'react';
import { useFleet, type Conn } from './useFleet';
import { sortedDevices } from './store';
import { STATES, type Device, type DeviceState } from './types';

const CONN_TEXT: Record<Conn, string> = {
  connecting: 'Connecting',
  connected: 'Live',
  disconnected: 'Disconnected, retrying',
};

export const Tile = memo(function Tile({ d }: { d: Device }) {
  return (
    <li className={`tile s-${d.state}`} data-testid={`device-tile-${d.deviceId}`} data-state={d.state}>
      <span className="id">{d.deviceId}</span>
      <span className="st">{d.state}</span>
    </li>
  );
});

export function App() {
  const { devices, summary, conn, ready } = useFleet();
  const [filter, setFilter] = useState<DeviceState | ''>('');
  const all = useMemo(() => sortedDevices(devices), [devices]);
  const shown = useMemo(() => (filter ? all.filter((d) => d.state === filter) : all), [all, filter]);

  return (
    <div data-testid="app-root" data-ready={ready ? 'true' : 'false'}>
      <header>
        <h1>FleetPulse</h1>
        <p role="status" className={`conn c-${conn}`} data-testid="connection-indicator" data-state={conn}>
          {CONN_TEXT[conn]}
        </p>
      </header>

      <section aria-label="Fleet summary" className="summary">
        <div data-testid="fleet-total">
          <b>{summary?.total ?? '-'}</b> total
        </div>
        {STATES.map((s) => (
          <div key={s} className={`s-${s}`} data-testid={`fleet-count-${s}`}>
            <b>{summary?.byState[s] ?? '-'}</b> {s}
          </div>
        ))}
      </section>

      <label className="filter">
        State{' '}
        <select
          data-testid="state-filter"
          value={filter}
          onChange={(e) => setFilter(e.target.value as DeviceState | '')}
        >
          <option value="">All</option>
          {STATES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>

      {!ready ? (
        <p data-testid="grid-loading">{conn === 'disconnected' ? 'Cannot reach the api.' : 'Loading devices...'}</p>
      ) : shown.length === 0 ? (
        <p data-testid="grid-empty">No devices{filter ? ` in ${filter}` : ''}.</p>
      ) : (
        <ul className="grid" data-testid="device-grid" aria-label={`Devices (${shown.length})`}>
          {shown.map((d) => (
            <Tile key={d.deviceId} d={d} />
          ))}
        </ul>
      )}
    </div>
  );
}
