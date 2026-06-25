<script>
  import { onMount } from 'svelte'

  let { processId = '', latency = [], counts = [] } = $props()

  let container = $state(null)
  let viewer = $state(null)
  let diagramXml = $state(null)
  let error = $state(null)
  let rendered = $state(false)

  function duration(ms) {
    const value = Number(ms || 0)
    if (value >= 60000) return `${(value / 60000).toFixed(1)}m`
    if (value >= 1000) return `${(value / 1000).toFixed(1)}s`
    return `${value}ms`
  }

  function latencyClass(p95, slaCount) {
    if (slaCount > 0) return 'overlay-danger'
    if (p95 > 30000) return 'overlay-warn'
    if (p95 > 5000) return 'overlay-slow'
    return 'overlay-ok'
  }

  function stateClass(state) {
    if (state === 'failed') return 'overlay-danger'
    if (state === 'cancelled') return 'overlay-warn'
    if (state === 'completed') return 'overlay-ok'
    return 'overlay-active'
  }

  function stateLabel(state) {
    if (!state) return ''
    return state.charAt(0).toUpperCase() + state.slice(1)
  }

  async function loadDiagram() {
    if (!processId) {
      error = 'No process selected'
      return
    }

    try {
      const response = await fetch('/api/diagram')
      if (!response.ok) {
        diagramXml = null
        error = response.status === 404 ? 'No diagram available for this process' : `Failed to load diagram (${response.status})`
        return
      }
      error = null
      diagramXml = await response.text()
    } catch (e) {
      error = `Cannot reach diagram endpoint: ${e.message}`
      diagramXml = null
    }
  }

  async function renderDiagram() {
    if (!container || !diagramXml) return

    if (viewer) {
      viewer.detach()
      viewer.destroy()
      viewer = null
    }

    const BpmnViewer = (await import('bpmn-js/lib/Viewer')).default
    viewer = new BpmnViewer({ container })

    try {
      await viewer.importXML(diagramXml)
      const canvas = viewer.get('canvas')
      canvas.zoom('fit-viewport')
      rendered = true
      applyOverlays()
    } catch (e) {
      error = `Failed to render diagram: ${e.message}`
      rendered = false
    }
  }

  function applyOverlays() {
    if (!viewer || !rendered) return

    const overlays = viewer.get('overlays')
    const elementRegistry = viewer.get('elementRegistry')
    overlays.clear()

    const latencyMap = new Map()
    for (const row of latency) {
      latencyMap.set(row.activityId, row)
    }

    const countMap = new Map()
    for (const row of counts) {
      if (row.state !== 'active' && row.state !== 'completed' && row.state !== 'failed' && row.state !== 'cancelled') {
        countMap.set(row.state, row)
      }
    }

    const terminalStates = new Set(['completed', 'failed', 'cancelled'])
    const lifecycleCounts = {}
    for (const row of counts) {
      if (terminalStates.has(row.state)) {
        lifecycleCounts[row.state] = (lifecycleCounts[row.state] || 0) + Number(row.count || 0)
      }
    }

    for (const element of elementRegistry.getAll()) {
      const id = element.businessObject?.id || element.id
      if (!id) continue

      const lat = latencyMap.get(id)
      const cnt = countMap.get(id)

      if (lat) {
        const cls = latencyClass(lat.p95DurationMs, lat.slaViolationCount)
        let html = `<div class="bpmn-overlay ${cls}"><span class="overlay-label">${escapeHtml(id)}</span>`
        html += `<span class="overlay-stat">n=${lat.sampleCount} avg=${duration(lat.averageDurationMs)} p95=${duration(lat.p95DurationMs)}</span>`
        if (lat.slaViolationCount > 0) {
          html += `<span class="overlay-sla">SLA:${lat.slaViolationCount}</span>`
        }
        html += '</div>'
        overlays.add(id, { position: { bottom: 0, left: 0 }, html })
      } else if (cnt && Number(cnt.count) > 0) {
        const cls = stateClass(cnt.state)
        let html = `<div class="bpmn-overlay ${cls}"><span class="overlay-label">${escapeHtml(id)}</span>`
        html += `<span class="overlay-stat">${stateLabel(cnt.state)}: ${cnt.count}</span>`
        html += '</div>'
        overlays.add(id, { position: { bottom: 0, left: 0 }, html })
      }
    }
  }

  function escapeHtml(text) {
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
  }

  $effect(() => {
    loadDiagram()
  })

  $effect(() => {
    if (diagramXml && container) {
      renderDiagram()
    }
  })

  $effect(() => {
    if (rendered && (latency.length || counts.length)) {
      applyOverlays()
    }
  })

  onMount(() => {
    return () => {
      if (viewer) {
        viewer.detach()
        viewer.destroy()
      }
    }
  })
</script>

<div class="diagram-panel">
  <div class="panel-title">
    <h2>Process Diagram</h2>
    <span>{processId || 'no process'}</span>
  </div>

  {#if error}
    <div class="diagram-error">{error}</div>
  {:else if !diagramXml}
    <div class="diagram-loading">Loading diagram...</div>
  {:else}
    <div class="diagram-legend">
      <span><i class="overlay-ok"></i>Healthy</span>
      <span><i class="overlay-slow"></i>Slow</span>
      <span><i class="overlay-warn"></i>Warning</span>
      <span><i class="overlay-danger"></i>Critical</span>
    </div>
  {/if}

  <div bind:this={container} class="diagram-container"></div>
</div>

<style>
  .diagram-panel {
    min-width: 0;
  }

  .panel-title {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
  }

  .panel-title h2 {
    margin: 0;
    font-size: 0.96rem;
    font-weight: 700;
  }

  .panel-title span {
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  .diagram-error {
    padding: 14px;
    border: 1px solid #f1b4af;
    border-radius: 8px;
    background: #fff1f0;
    color: #8f1f17;
    font-size: 0.88rem;
    margin-bottom: 10px;
  }

  .diagram-loading {
    padding: 32px;
    text-align: center;
    color: var(--muted);
    font-size: 0.9rem;
  }

  .diagram-container {
    width: 100%;
    height: 550px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: #fff;
    overflow: hidden;
  }

  .diagram-container :global(.bjs-container) {
    width: 100%;
    height: 100%;
  }

  .diagram-container :global(svg) {
    width: 100%;
    height: 100%;
  }

  .diagram-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 8px;
    color: var(--muted);
    font-size: 0.75rem;
    font-weight: 700;
  }

  .diagram-legend span {
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }

  .diagram-legend i {
    width: 10px;
    height: 10px;
    border-radius: 3px;
    display: inline-block;
  }

  .diagram-legend i.overlay-ok { background: #12805c; }
  .diagram-legend i.overlay-slow { background: #2563eb; }
  .diagram-legend i.overlay-warn { background: #b7791f; }
  .diagram-legend i.overlay-danger { background: #c7372f; }

  :global(.bpmn-overlay) {
    padding: 3px 8px;
    border-radius: 6px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 0.7rem;
    white-space: nowrap;
    display: flex;
    flex-direction: column;
    gap: 1px;
    pointer-events: none;
  }

  :global(.bpmn-overlay.overlay-ok) {
    background: rgba(18, 128, 92, 0.12);
    border: 1px solid rgba(18, 128, 92, 0.35);
    color: #0d5c43;
  }

  :global(.bpmn-overlay.overlay-slow) {
    background: rgba(37, 99, 235, 0.12);
    border: 1px solid rgba(37, 99, 235, 0.35);
    color: #1a4fb8;
  }

  :global(.bpmn-overlay.overlay-warn) {
    background: rgba(183, 121, 31, 0.12);
    border: 1px solid rgba(183, 121, 31, 0.35);
    color: #8a5917;
  }

  :global(.bpmn-overlay.overlay-danger) {
    background: rgba(199, 55, 47, 0.12);
    border: 1px solid rgba(199, 55, 47, 0.35);
    color: #9a211a;
  }

  :global(.bpmn-overlay.overlay-active) {
    background: rgba(14, 116, 144, 0.12);
    border: 1px solid rgba(14, 116, 144, 0.35);
    color: #0a5569;
  }

  :global(.overlay-label) {
    font-weight: 800;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }

  :global(.overlay-stat) {
    font-size: 0.62rem;
    opacity: 0.85;
  }

  :global(.overlay-sla) {
    font-weight: 800;
    color: #c7372f;
    font-size: 0.62rem;
  }
</style>
