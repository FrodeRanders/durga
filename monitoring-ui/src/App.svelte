<script>
  import { getState } from './stores.svelte.js'

  const s = getState()

  let mounted = false
  $effect(() => {
    if (!mounted) {
      mounted = true
      s.scheduleRefresh(() => s.refresh())
    }
  })
</script>

<div class="app">
  <header>
    <span class="pill">Durga Monitoring</span>
    <h1>Process traffic, state, and latency in one place.</h1>
    <p class="muted">Live view backed by the Kafka Streams query stores.</p>
  </header>

  <section class="controls">
    <label>
      <span>Process ID</span>
      <input
        type="text"
        bind:value={s.processId}
        onchange={() => s.scheduleRefresh(() => s.refresh())}
      />
    </label>
    <label>
      <span>Stuck Threshold (s)</span>
      <input
        type="number"
        min="1"
        bind:value={s.threshold}
        onchange={() => s.scheduleRefresh(() => s.refresh())}
      />
    </label>
    <label>
      <span>Refresh (s)</span>
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

  <section class="grid">
    <article class="panel">
      <h2>Health</h2>
      <div class="stat">{s.health?.streamsState ?? '...'}</div>
    </article>

    <article class="panel">
      <h2>Counts</h2>
      {#if s.counts.length}
        <table>
          <thead><tr><th>State</th><th>Count</th></tr></thead>
          <tbody>
            {#each s.counts as row}
              <tr><td>{row.state}</td><td>{row.count}</td></tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="muted">No data</p>
      {/if}
    </article>

    <article class="panel">
      <h2>Latency</h2>
      {#if s.latency.length}
        <table>
          <thead><tr><th>Activity</th><th>Samples</th><th>Avg ms</th><th>Max ms</th></tr></thead>
          <tbody>
            {#each s.latency as row}
              <tr>
                <td>{row.activityId}</td>
                <td>{row.sampleCount}</td>
                <td>{row.averageDurationMs?.toFixed(1)}</td>
                <td>{row.maxDurationMs}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="muted">No data</p>
      {/if}
    </article>

    <article class="panel">
      <h2>Stuck Instances</h2>
      {#if s.stuck.length}
        <table>
          <thead><tr><th>Instance</th><th>Activity</th><th>Age s</th><th>State</th></tr></thead>
          <tbody>
            {#each s.stuck as row}
              <tr>
                <td>
                  <button
                    class="link"
                    onclick={() => { s.instanceId = row.processInstanceId; s.refreshInstance() }}
                  >
                    {row.processInstanceId.slice(0, 10)}...
                  </button>
                </td>
                <td>{row.currentActivityId ?? '-'}</td>
                <td>{row.ageSeconds}</td>
                <td>{row.lifecycleState}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="muted">No stuck instances</p>
      {/if}
    </article>

    <article class="panel wide">
      <h2>Selected Instance</h2>
      <p class="muted">Click an instance from the stuck table or paste an ID.</p>
      <label>
        <span>Process Instance ID</span>
        <input
          type="text"
          bind:value={s.instanceId}
          onchange={() => s.refreshInstance()}
          placeholder="paste a processInstanceId"
        />
      </label>
      {#if s.instanceView}
        <pre>{JSON.stringify(s.instanceView, null, 2)}</pre>
      {:else}
        <pre class="muted">No instance selected.</pre>
      {/if}
    </article>
  </section>
</div>

<style>
  :root {
    --bg: #f4efe6;
    --panel: #fffaf0;
    --ink: #1f1a17;
    --muted: #6b6258;
    --line: #d8c7ae;
    --accent: #b4542f;
    --accent-2: #2f6c63;
  }
  * { box-sizing: border-box; }
  :global(body) {
    margin: 0;
    font-family: system-ui, -apple-system, sans-serif;
    color: var(--ink);
    background:
      radial-gradient(circle at top left, #f7d7b2 0, transparent 28rem),
      radial-gradient(circle at bottom right, #d8eadf 0, transparent 30rem),
      var(--bg);
  }
  .app {
    max-width: 1100px;
    margin: 0 auto;
    padding: 32px 18px 60px;
  }
  h1, h2 { margin: 0; font-weight: 700; }
  .muted { color: var(--muted); }
  header { margin-bottom: 22px; }
  header h1 { font-size: clamp(2.2rem, 5vw, 4rem); line-height: 0.95; }
  .pill {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    background: #efe1cf;
    color: var(--accent-2);
    font-size: 0.82rem;
    margin-bottom: 12px;
  }
  .controls {
    display: grid;
    gap: 16px;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    margin-bottom: 18px;
  }
  label { display: grid; gap: 6px; font-size: 0.9rem; color: var(--muted); }
  label span { font-weight: 600; }
  input {
    padding: 12px 14px;
    border: 1px solid var(--line);
    border-radius: 14px;
    background: rgba(255,255,255,0.9);
    color: var(--ink);
    font-size: 0.95rem;
  }
  .grid {
    display: grid;
    gap: 16px;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  }
  .wide { grid-column: 1 / -1; }
  .panel {
    background: rgba(255,250,240,0.88);
    border: 1px solid var(--line);
    border-radius: 22px;
    padding: 18px;
    box-shadow: 0 10px 30px rgba(70, 44, 24, 0.08);
    backdrop-filter: blur(6px);
  }
  .stat { font-size: 2rem; color: var(--accent); margin-top: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.9rem; margin-top: 8px; }
  th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); }
  th { color: var(--muted); font-weight: 600; }
  button.link {
    background: none;
    border: none;
    color: var(--accent);
    cursor: pointer;
    font: inherit;
    text-decoration: underline;
  }
  pre {
    margin: 8px 0 0;
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 0.88rem;
  }
  .error {
    background: #fce4e4;
    border: 1px solid #c44;
    border-radius: 14px;
    padding: 14px 18px;
    margin-bottom: 16px;
    color: #822;
  }
</style>
