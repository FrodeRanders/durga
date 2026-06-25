<script>
  import { onMount } from 'svelte'
  import { getState } from './stores.svelte.js'
  import BpmnDiagram from './BpmnDiagram.svelte'

  const s = getState()

  const terminalStates = new Set(['completed', 'failed', 'cancelled'])
  const trendMetrics = ['started', 'completed', 'failed']

  window.addEventListener('unhandledrejection', (e) => {
    console.error('[durga] unhandled rejection', e.reason)
  })
  window.addEventListener('error', (e) => {
    console.error('[durga] runtime error', e.message)
  })

  onMount(() => {
    s.discoverProcessId().then(() => {
      s.scheduleRefresh(() => s.refresh())
    })
    s.checkDiagramAvailable()
  })

  function number(value) {
    return new Intl.NumberFormat().format(Number(value || 0))
  }

  function duration(ms) {
    const value = Number(ms || 0)
    if (value >= 60000) return `${(value / 60000).toFixed(1)}m`
    if (value >= 1000) return `${(value / 1000).toFixed(1)}s`
    return `${value}ms`
  }

  function dateTime(value) {
    if (!value) return '-'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return value
    return date.toLocaleString()
  }

  function timeLabel(value) {
    if (!value) return '-'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return value
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  }

  function percent(value, max, minimum = 0) {
    const numericValue = Number(value || 0)
    const numericMax = Number(max || 0)
    if (!Number.isFinite(numericValue) || !Number.isFinite(numericMax) || numericMax <= 0) {
      return `${minimum}%`
    }
    return `${Math.max(minimum, Math.min(100, (numericValue / numericMax) * 100))}%`
  }

  function processRows() {
    const grouped = new Map()
    for (const row of s.allCounts) {
      const processId = row.processId || 'unknown'
      const current = grouped.get(processId) || {
        processId,
        total: 0,
        active: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        states: 0
      }
      const count = Number(row.count || 0)
      current.total += count
      current.states += 1
      if (row.state === 'completed') current.completed += count
      else if (row.state === 'failed') current.failed += count
      else if (row.state === 'cancelled') current.cancelled += count
      else current.active += count
      grouped.set(processId, current)
    }
    return Array.from(grouped.values()).sort((a, b) => b.total - a.total)
  }

  function totals() {
    return processRows().reduce((acc, row) => ({
      processes: acc.processes + 1,
      instances: acc.instances + row.total,
      active: acc.active + row.active,
      failed: acc.failed + row.failed,
      completed: acc.completed + row.completed,
      cancelled: acc.cancelled + row.cancelled
    }), { processes: 0, instances: 0, active: 0, failed: 0, completed: 0, cancelled: 0 })
  }

  function stateRows() {
    return [...s.counts]
      .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
      .map((row) => ({
        ...row,
        className: terminalStates.has(row.state) ? row.state : 'active'
      }))
  }

  function selectedProcessTotal() {
    return Math.max(1, s.counts.reduce((sum, row) => sum + Number(row.count || 0), 0))
  }

  function slowestActivities() {
    return [...s.latency].sort((a, b) => Number(b.p95DurationMs || 0) - Number(a.p95DurationMs || 0))
  }

  function latencyMax() {
    return Math.max(1, ...s.latency.map((row) => Number(row.p95DurationMs || row.averageDurationMs || 0)))
  }

  function trendBuckets() {
    const byBucket = new Map()
    for (const point of s.trends) {
      const bucket = point.bucketStartedAt || ''
      if (!bucket || Number.isNaN(new Date(bucket).getTime())) {
        continue
      }
      const row = byBucket.get(bucket) || { bucket, started: 0, completed: 0, failed: 0 }
      if (trendMetrics.includes(point.metric)) {
        row[point.metric] += Number(point.count || 0)
      }
      byBucket.set(bucket, row)
    }
    return Array.from(byBucket.values())
      .sort((a, b) => a.bucket.localeCompare(b.bucket))
      .slice(-12)
  }

  function trendMax() {
    return Math.max(1, ...trendBuckets().flatMap((row) => trendMetrics.map((metric) => row[metric])))
  }

  function selectProcess(processId) {
    s.processId = processId
    s.scheduleRefresh(() => s.refresh())
  }

  function selectInstance(instanceId) {
    s.lookupInstance(instanceId)
  }
</script>

<div class="app">
  <header class="topbar">
    <div>
      <p class="eyebrow">Durga Monitoring</p>
      <h1>Kafka Process Dashboard</h1>
    </div>
    <div class="status {s.health?.streamsState === 'RUNNING' ? 'ok' : 'warn'}">
      <span></span>
      {s.health?.streamsState ?? 'UNKNOWN'}
    </div>
  </header>

  <section class="controls" aria-label="Dashboard filters">
    <label>
      <span>Process</span>
      <input
        type="text"
        list="processes"
        bind:value={s.processId}
        onchange={() => s.scheduleRefresh(() => s.refresh())}
      />
      <datalist id="processes">
        {#each processRows() as row}
          <option value={row.processId}></option>
        {/each}
      </datalist>
    </label>
    <label>
      <span>Stuck age</span>
      <input
        type="number"
        min="1"
        bind:value={s.threshold}
        onchange={() => s.scheduleRefresh(() => s.refresh())}
      />
    </label>
    <label>
      <span>Refresh</span>
      <input
        type="number"
        min="1"
        bind:value={s.refreshSecs}
        onchange={() => s.scheduleRefresh(() => s.refresh())}
      />
    </label>
  </section>

  {#if s.error}
    <section class="error">{s.error}</section>
  {/if}

  <section class="kpis" aria-label="Cluster summary">
    <article class="kpi">
      <span>Processes</span>
      <strong>{number(totals().processes)}</strong>
    </article>
    <article class="kpi">
      <span>Instances</span>
      <strong>{number(totals().instances)}</strong>
    </article>
    <article class="kpi">
      <span>Active</span>
      <strong>{number(totals().active)}</strong>
    </article>
    <article class="kpi alert">
      <span>Failed</span>
      <strong>{number(totals().failed)}</strong>
    </article>
    <article class="kpi">
      <span>Stuck</span>
      <strong>{number(s.stuck.length)}</strong>
    </article>
  </section>

  <main class="layout">
    <section class="panel process-list">
      <div class="panel-title">
        <h2>Process Inventory</h2>
        <span>{number(processRows().length)} process ids</span>
      </div>
      {#if processRows().length}
        <table>
          <thead><tr><th>Process</th><th>Total</th><th>Active</th><th>Failed</th></tr></thead>
          <tbody>
            {#each processRows() as row}
              <tr class:selected={row.processId === s.processId}>
                <td>
                  <button class="link" onclick={() => selectProcess(row.processId)}>
                    {row.processId}
                  </button>
                </td>
                <td>{number(row.total)}</td>
                <td>{number(row.active)}</td>
                <td class:danger={row.failed > 0}>{number(row.failed)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="empty">No process counts available.</p>
      {/if}
    </section>

    <section class="panel state-mix">
      <div class="panel-title">
        <h2>Current State Mix</h2>
        <span>{s.processId}</span>
      </div>
      {#if stateRows().length}
        <div class="state-bars">
          {#each stateRows() as row}
            <div class="state-row">
              <div class="state-label">
                <span class="badge {row.className}">{row.state}</span>
                <strong>{number(row.count)}</strong>
              </div>
              <div class="bar-track">
                <div
                  class="bar-fill {row.className}"
                  style={`width: ${percent(row.count, selectedProcessTotal(), 4)}`}
                ></div>
              </div>
            </div>
          {/each}
        </div>
      {:else}
        <p class="empty">No state counts for this process.</p>
      {/if}
    </section>

    <section class="panel trend-panel">
      <div class="panel-title">
        <h2>Lifecycle Trend</h2>
        <span>minute buckets</span>
      </div>
      {#if trendBuckets().length}
        <div class="trend">
          {#each trendBuckets() as row}
            <div class="trend-column" title={dateTime(row.bucket)}>
              <div class="stack">
                <span class="segment failed" style={`height: ${percent(row.failed, trendMax(), 0)}`}></span>
                <span class="segment completed" style={`height: ${percent(row.completed, trendMax(), 0)}`}></span>
                <span class="segment started" style={`height: ${percent(row.started, trendMax(), 0)}`}></span>
              </div>
              <small>{timeLabel(row.bucket)}</small>
            </div>
          {/each}
        </div>
        <div class="legend">
          <span><i class="started"></i>started</span>
          <span><i class="completed"></i>completed</span>
          <span><i class="failed"></i>failed</span>
        </div>
      {:else}
        <p class="empty">No trend buckets for this process.</p>
      {/if}
    </section>

    <section class="panel latency-panel">
      <div class="panel-title">
        <h2>Activity Latency</h2>
        <span>ordered by p95</span>
      </div>
      {#if slowestActivities().length}
        <table>
          <thead>
            <tr>
              <th>Activity</th>
              <th>Samples</th>
              <th>Avg</th>
              <th>P95</th>
              <th>P99</th>
              <th>SLA</th>
            </tr>
          </thead>
          <tbody>
            {#each slowestActivities() as row}
              <tr>
                <td>
                  <div class="activity-cell">
                    <span>{row.activityId}</span>
                    <div class="bar-track slim">
                      <div
                        class="bar-fill latency"
                        style={`width: ${percent(row.p95DurationMs, latencyMax(), 0)}`}
                      ></div>
                    </div>
                  </div>
                </td>
                <td>{number(row.sampleCount)}</td>
                <td>{duration(row.averageDurationMs)}</td>
                <td>{duration(row.p95DurationMs)}</td>
                <td>{duration(row.p99DurationMs)}</td>
                <td class:danger={row.slaViolationCount > 0}>{number(row.slaViolationCount)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="empty">No completed activity samples for this process.</p>
      {/if}
    </section>

    <section class="panel stuck-panel">
      <div class="panel-title">
        <h2>Stuck Instances</h2>
        <span>&gt; {number(s.threshold)}s</span>
      </div>
      {#if s.stuck.length}
        <table>
          <thead><tr><th>Instance</th><th>Activity</th><th>Age</th><th>Updated</th></tr></thead>
          <tbody>
            {#each s.stuck as row}
              <tr>
                <td>
                  <button class="link mono" onclick={() => selectInstance(row.processInstanceId)}>
                    {row.processInstanceId}
                  </button>
                </td>
                <td>{row.currentActivityId ?? '-'}</td>
                <td>{number(row.ageSeconds)}s</td>
                <td>{dateTime(row.lastUpdatedAt)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="empty">No stuck instances at the current threshold.</p>
      {/if}
    </section>

    {#if s.diagramAvailable}
      <section class="panel wide">
        <BpmnDiagram processId={s.processId} latency={s.latency} counts={s.counts} />
      </section>
    {/if}

    <section class="panel instance-panel">
      <div class="panel-title">
        <h2>Instance Detail</h2>
        <span>{s.instanceView?.lifecycleState ?? 'none selected'}</span>
      </div>
      <label class="instance-input">
        <span>Instance id</span>
        <input
          type="text"
          bind:value={s.instanceId}
          oninput={(e) => s.lookupInstance(e.target.value)}
        />
      </label>
      {#if s.instanceView}
        <dl class="details">
          <div><dt>Process</dt><dd>{s.instanceView.processId ?? '-'}</dd></div>
          <div><dt>Activity</dt><dd>{s.instanceView.currentActivityId ?? '-'}</dd></div>
          <div><dt>Started</dt><dd>{dateTime(s.instanceView.startedAt)}</dd></div>
          <div><dt>Updated</dt><dd>{dateTime(s.instanceView.lastUpdatedAt)}</dd></div>
          <div><dt>Completed</dt><dd>{dateTime(s.instanceView.completedAt)}</dd></div>
          <div><dt>Retries</dt><dd>{number(s.instanceView.retryCount)}</dd></div>
          <div><dt>Business key</dt><dd>{s.instanceView.businessKey ?? '-'}</dd></div>
          <div><dt>Correlation</dt><dd>{s.instanceView.correlationId ?? '-'}</dd></div>
        </dl>
        {#if s.instanceView.lastErrorCode || s.instanceView.lastErrorMessage}
          <div class="error-block">
            <strong>{s.instanceView.lastErrorCode ?? 'error'}</strong>
            <span>{s.instanceView.lastErrorMessage}</span>
          </div>
        {/if}
      {:else}
        <p class="empty">No instance selected.</p>
      {/if}
    </section>
  </main>
</div>

<style>
  :root {
    --bg: #f7f9fc;
    --surface: #ffffff;
    --surface-soft: #eef4f8;
    --ink: #18212f;
    --muted: #667085;
    --line: #d9e2ec;
    --blue: #2563eb;
    --green: #12805c;
    --red: #c7372f;
    --amber: #b7791f;
    --cyan: #0e7490;
    --shadow: 0 10px 28px rgba(31, 41, 55, 0.08);
  }

  * { box-sizing: border-box; }

  :global(body) {
    margin: 0;
    color: var(--ink);
    background: var(--bg);
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }

  .app {
    max-width: 1440px;
    margin: 0 auto;
    padding: 22px;
  }

  .topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    margin-bottom: 18px;
  }

  h1, h2, p { margin: 0; }

  h1 {
    font-size: 1.7rem;
    font-weight: 720;
    letter-spacing: 0;
  }

  h2 {
    font-size: 0.96rem;
    font-weight: 700;
  }

  .eyebrow {
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-height: 34px;
    padding: 0 12px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--surface);
    color: var(--muted);
    font-size: 0.82rem;
    font-weight: 700;
  }

  .status span {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--amber);
  }

  .status.ok span { background: var(--green); }
  .status.warn span { background: var(--amber); }

  .controls {
    display: grid;
    grid-template-columns: minmax(220px, 1fr) 150px 140px;
    gap: 12px;
    margin-bottom: 14px;
  }

  label {
    display: grid;
    gap: 5px;
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  input {
    width: 100%;
    height: 38px;
    padding: 0 10px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--surface);
    color: var(--ink);
    font-size: 0.9rem;
  }

  input:focus {
    outline: 2px solid rgba(37, 99, 235, 0.18);
    border-color: var(--blue);
  }

  .error {
    margin-bottom: 14px;
    padding: 12px 14px;
    border: 1px solid #f1b4af;
    border-radius: 8px;
    background: #fff1f0;
    color: #8f1f17;
    font-size: 0.9rem;
  }

  .kpis {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 14px;
  }

  .kpi {
    min-height: 82px;
    padding: 14px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--surface);
    box-shadow: var(--shadow);
  }

  .kpi span {
    display: block;
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  .kpi strong {
    display: block;
    margin-top: 8px;
    font-size: 1.8rem;
    line-height: 1;
  }

  .kpi.alert strong { color: var(--red); }

  .layout {
    display: grid;
    grid-template-columns: minmax(280px, 0.95fr) minmax(340px, 1.2fr) minmax(320px, 1fr);
    gap: 14px;
  }

  .panel {
    min-width: 0;
    padding: 14px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--surface);
    box-shadow: var(--shadow);
  }

  .wide { grid-column: span 2; }

  .panel-title {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 12px;
  }

  .panel-title span {
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.86rem;
  }

  th, td {
    padding: 9px 8px;
    border-bottom: 1px solid var(--line);
    text-align: left;
    vertical-align: middle;
  }

  th {
    color: var(--muted);
    font-size: 0.72rem;
    font-weight: 800;
    text-transform: uppercase;
  }

  tr.selected td { background: #edf4ff; }

  .link {
    max-width: 100%;
    padding: 0;
    border: 0;
    background: transparent;
    color: var(--blue);
    cursor: pointer;
    font: inherit;
    font-weight: 700;
    text-align: left;
  }

  .mono {
    max-width: 190px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 0.78rem;
  }

  .danger { color: var(--red); font-weight: 800; }

  .empty {
    padding: 22px 0;
    color: var(--muted);
    font-size: 0.88rem;
  }

  .state-bars {
    display: grid;
    gap: 13px;
  }

  .state-label {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 6px;
    font-size: 0.86rem;
  }

  .badge {
    display: inline-flex;
    align-items: center;
    max-width: 78%;
    padding: 3px 8px;
    border-radius: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    background: var(--surface-soft);
    color: var(--cyan);
    font-weight: 800;
  }

  .badge.completed { color: var(--green); }
  .badge.failed { color: var(--red); }
  .badge.cancelled { color: var(--amber); }

  .bar-track {
    height: 9px;
    overflow: hidden;
    border-radius: 8px;
    background: #e7edf4;
  }

  .bar-track.slim {
    height: 5px;
    margin-top: 6px;
  }

  .bar-fill {
    height: 100%;
    border-radius: inherit;
    background: var(--cyan);
  }

  .bar-fill.completed { background: var(--green); }
  .bar-fill.failed { background: var(--red); }
  .bar-fill.cancelled { background: var(--amber); }
  .bar-fill.latency { background: var(--blue); }

  .trend {
    display: grid;
    grid-template-columns: repeat(12, minmax(18px, 1fr));
    align-items: end;
    gap: 8px;
    min-height: 178px;
  }

  .trend-column {
    display: grid;
    gap: 6px;
    align-items: end;
    min-width: 0;
  }

  .stack {
    display: flex;
    flex-direction: column-reverse;
    justify-content: flex-start;
    height: 140px;
    overflow: hidden;
    border-radius: 8px;
    background: #e7edf4;
  }

  .segment {
    display: block;
    min-height: 2px;
  }

  .segment.started, .legend .started { background: var(--blue); }
  .segment.completed, .legend .completed { background: var(--green); }
  .segment.failed, .legend .failed { background: var(--red); }

  .trend small {
    overflow: hidden;
    color: var(--muted);
    font-size: 0.68rem;
    text-align: center;
    white-space: nowrap;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-top: 12px;
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  .legend span {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }

  .legend i {
    width: 10px;
    height: 10px;
    border-radius: 3px;
  }

  .activity-cell {
    min-width: 180px;
  }

  .instance-input {
    margin-bottom: 12px;
  }

  .details {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px 14px;
    margin: 0;
  }

  .details div {
    min-width: 0;
  }

  dt {
    color: var(--muted);
    font-size: 0.72rem;
    font-weight: 800;
    text-transform: uppercase;
  }

  dd {
    margin: 3px 0 0;
    overflow-wrap: anywhere;
    font-size: 0.86rem;
  }

  .error-block {
    display: grid;
    gap: 4px;
    margin-top: 12px;
    padding: 10px;
    border-radius: 8px;
    background: #fff1f0;
    color: #8f1f17;
    font-size: 0.86rem;
  }

  @media (max-width: 1050px) {
    .layout {
      grid-template-columns: 1fr 1fr;
    }

    .wide {
      grid-column: 1 / -1;
    }

    .kpis {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
  }

  @media (max-width: 760px) {
    .app {
      padding: 14px;
    }

    .topbar,
    .controls {
      grid-template-columns: 1fr;
    }

    .topbar {
      align-items: flex-start;
      flex-direction: column;
    }

    .kpis,
    .layout,
    .details {
      grid-template-columns: 1fr;
    }

    .wide {
      grid-column: auto;
    }

    .panel,
    .kpi {
      box-shadow: none;
    }

    table {
      font-size: 0.8rem;
    }

    th, td {
      padding: 8px 6px;
    }
  }
</style>
