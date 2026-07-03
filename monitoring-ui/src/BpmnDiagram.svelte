<script>
  let { processId = '', latency = [], counts = [], alarms = [] } = $props()

  let container = $state(null)
  let viewer = null
  let canvasModule = null
  let diagramXml = $state(null)
  let diagramError = $state(null)
  let rendered = $state(false)
  let loadedForProcessId = null
  let panMode = $state(false)
  let panning = false
  let panLast = { x: 0, y: 0 }

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

  function alarmClass(severity) {
    return severity === 'CRITICAL' ? 'overlay-danger' : 'overlay-warn'
  }

  function strongerAlarm(current, next) {
    if (!current) return next
    if (next.severity === 'CRITICAL' && current.severity !== 'CRITICAL') return next
    if (current.severity === 'CRITICAL' && next.severity !== 'CRITICAL') return current
    return String(next.lastTriggeredAt || '').localeCompare(String(current.lastTriggeredAt || '')) > 0 ? next : current
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

    const alarmMap = new Map()
    for (const alarm of alarms) {
      if (alarm.activityId) {
        alarmMap.set(alarm.activityId, strongerAlarm(alarmMap.get(alarm.activityId), alarm))
      }
    }

    for (const element of elementRegistry.getAll()) {
      const id = element.businessObject?.id || element.id
      if (!id) continue

      const alarm = alarmMap.get(id)
      const lat = latencyMap.get(id)
      const cnt = countMap.get(id)

      if (alarm) {
        const cls = alarmClass(alarm.severity)
        let html = `<div class="bpmn-overlay ${cls}"><span class="overlay-label">${escapeHtml(id)}</span>`
        html += `<span class="overlay-stat">${escapeHtml(alarm.severity)} alarm x${alarm.fireCount || 1}</span>`
        if (alarm.lastMessage) {
          html += `<span class="overlay-sla">${escapeHtml(alarm.lastMessage)}</span>`
        }
        html += '</div>'
        overlays.add(id, { position: { bottom: 0, left: 0 }, html })
      } else if (lat) {
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

  function zoomIn() {
    if (!canvasModule) return
    const current = canvasModule.zoom()
    canvasModule.zoom(current * 1.2, { x: container.clientWidth / 2, y: container.clientHeight / 2 })
  }

  function zoomOut() {
    if (!canvasModule) return
    const current = canvasModule.zoom()
    canvasModule.zoom(current * 0.8, { x: container.clientWidth / 2, y: container.clientHeight / 2 })
  }

  function fitViewport() {
    if (!canvasModule) return
    canvasModule.zoom('fit-viewport')
  }

  function togglePan() {
    panMode = !panMode
  }

  function onPanDown(e) {
    if (!panMode || !canvasModule) return
    panning = true
    panLast = { x: e.clientX, y: e.clientY }
    e.preventDefault()
  }

  function onPanMove(e) {
    if (!panning || !canvasModule) return
    const dx = e.clientX - panLast.x
    const dy = e.clientY - panLast.y
    canvasModule.scroll({ dx, dy })
    panLast = { x: e.clientX, y: e.clientY }
    e.preventDefault()
  }

  function onPanUp() {
    panning = false
  }

  // Apply overlays reactively when data changes (but only after initial render)
  $effect(() => {
    if (rendered) {
      applyOverlays()
    }
  })

  const MAX_RETRIES = 12
  const RETRY_INTERVAL_MS = 5000
  let retryTimer = null
  let retryCount = 0

  function clearRetry() {
    if (retryTimer) {
      clearInterval(retryTimer)
      retryTimer = null
    }
    retryCount = 0
  }

  function scheduleRetry() {
    if (retryTimer) return
    retryTimer = setInterval(() => {
      retryCount++
      if (retryCount > MAX_RETRIES) {
        clearRetry()
        diagramError = 'Model not available — try reselecting the process'
        return
      }
      fetchAndRender()
    }, RETRY_INTERVAL_MS)
  }

  async function fetchAndRender() {
    if (!processId) {
      diagramError = 'No process selected'
      return
    }

    // 1. Fetch diagram XML
    try {
      const pid = encodeURIComponent(processId || '')
      const url = pid ? `/api/diagram?processId=${pid}` : '/api/diagram'
      const response = await fetch(url)
      if (!response.ok) {
        if (response.status === 404) {
          diagramError = 'Waiting for model to become available...'
          scheduleRetry()
        } else {
          diagramError = `HTTP ${response.status}`
        }
        return
      }
      diagramXml = await response.text()
      clearRetry()
    } catch (e) {
      diagramError = `Cannot reach diagram endpoint: ${e.message}`
      scheduleRetry()
      return
    }

    if (loadedForProcessId === processId) return
    loadedForProcessId = processId

    // 2. Render diagram (runs once)
    try {
      const BpmnViewer = (await import('bpmn-js/lib/Viewer')).default
      viewer = new BpmnViewer({ container })
      canvasModule = viewer.get('canvas')
      await viewer.importXML(diagramXml)
      requestAnimationFrame(() => {
        canvasModule.resized()
        canvasModule.zoom('fit-viewport')
      })
      rendered = true
      diagramError = null
      applyOverlays()
    } catch (e) {
      diagramError = `Failed to render diagram: ${e.message}`
    }
  }

  $effect(() => {
    const pid = processId

    clearRetry()
    diagramXml = null
    diagramError = null
    rendered = false
    loadedForProcessId = null
    if (viewer) {
      viewer.detach()
      viewer.destroy()
      viewer = null
      canvasModule = null
    }

    if (pid) {
      fetchAndRender()
    }

    return () => {
      clearRetry()
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

  {#if diagramError}
    <div class="diagram-error">{diagramError}</div>
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

  <div class="diagram-wrapper">
    <div
      bind:this={container}
      class="diagram-container"
      class:pan-active={panMode}
      role="application"
      aria-label="BPMN diagram"
      onmousedown={onPanDown}
      onmousemove={onPanMove}
      onmouseup={onPanUp}
      onmouseleave={onPanUp}
    ></div>
    {#if rendered}
      <div class="diagram-toolbar">
        <button class:active={panMode} onclick={togglePan} title="Pan (grab to move)">&#x1F590;</button>
        <button onclick={zoomIn} title="Zoom in">+</button>
        <button onclick={zoomOut} title="Zoom out">&minus;</button>
        <button onclick={fitViewport} title="Fit to view">&#x2316;</button>
      </div>
    {/if}
  </div>
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

  .diagram-wrapper {
    position: relative;
  }

  .diagram-container {
    width: 100%;
    height: 550px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: #fff;
    overflow: hidden;
    touch-action: none;
    cursor: default;
  }

  .diagram-container.pan-active {
    cursor: grab;
  }

  .diagram-container.pan-active:active {
    cursor: grabbing;
  }

  .diagram-container :global(.bjs-container) {
    width: 100%;
    height: 100%;
  }

  .diagram-container :global(svg) {
    width: 100%;
    height: 100%;
  }

  .diagram-toolbar {
    position: absolute;
    top: 10px;
    right: 10px;
    display: flex;
    gap: 4px;
    z-index: 10;
  }

  .diagram-toolbar button {
    width: 32px;
    height: 32px;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: rgba(255, 255, 255, 0.9);
    color: #333;
    font-size: 1.1rem;
    font-weight: 700;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    line-height: 1;
    backdrop-filter: blur(4px);
    transition: background 0.15s;
  }

  .diagram-toolbar button:hover {
    background: rgba(240, 240, 240, 0.95);
  }

  .diagram-toolbar button.active {
    background: #e0e7ff;
    border-color: #818cf8;
    color: #3730a3;
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
