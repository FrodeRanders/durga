// Pure helpers for building BPMN diagram overlays. Kept free of DOM/bpmn-js so
// the overlay model can be unit-tested.

export function duration(ms) {
  const value = Number(ms || 0)
  if (value >= 60000) return `${(value / 60000).toFixed(1)}m`
  if (value >= 1000) return `${(value / 1000).toFixed(1)}s`
  return `${value}ms`
}

export function count(value) {
  return new Intl.NumberFormat().format(Number(value || 0))
}

export function latencyClass(p95, slaCount) {
  if (Number(slaCount) > 0) return 'overlay-danger'
  if (Number(p95) > 30000) return 'overlay-warn'
  if (Number(p95) > 5000) return 'overlay-slow'
  return 'overlay-ok'
}

export function alarmClass(severity) {
  return severity === 'CRITICAL' ? 'overlay-danger' : 'overlay-warn'
}

// Picks the more serious of two alarms for the same activity: CRITICAL wins over
// non-critical, otherwise the most recently triggered.
export function strongerAlarm(current, next) {
  if (!current) return next
  if (next.severity === 'CRITICAL' && current.severity !== 'CRITICAL') return next
  if (current.severity === 'CRITICAL' && next.severity !== 'CRITICAL') return current
  return String(next.lastTriggeredAt || '').localeCompare(String(current.lastTriggeredAt || '')) > 0
    ? next
    : current
}

export function escapeHtml(text) {
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/**
 * Builds the overlay model for one task node from its aggregate statistics and
 * (optionally) an active alarm.
 *
 * The overlay favours aggregate task statistics — how many items the task has
 * processed (throughput) and its latency profile — rather than any single
 * execution. Throughput comes from completion counts (available even when latency
 * is not), while latency adds the performance profile when entry/exit pairs exist.
 * When an alarm is active it is layered on top: it drives the overlay colour
 * (alarms outrank latency-derived severity) and adds explicit alarm lines.
 *
 * @param id activity/element id
 * @param processed items processed for this activity (throughput count), or 0/undefined
 * @param lat latency summary for this activity, or undefined
 * @param alarm active alarm for this activity, or undefined
 * @returns overlay descriptor {cls, label, lines[]} or null when nothing to show
 */
export function buildTaskOverlay(id, processed, lat, alarm) {
  const hasLat = lat && Number(lat.sampleCount) > 0
  const itemsProcessed = Number(processed || 0) > 0
    ? Number(processed)
    : (hasLat ? Number(lat.sampleCount) : 0)

  if (itemsProcessed === 0 && !alarm) return null

  const cls = alarm
    ? alarmClass(alarm.severity)
    : (hasLat ? latencyClass(lat.p95DurationMs, lat.slaViolationCount) : 'overlay-ok')

  const lines = []
  if (itemsProcessed > 0) {
    lines.push({ cls: 'overlay-stat', text: `processed ${count(itemsProcessed)}` })
  }
  if (hasLat) {
    lines.push({ cls: 'overlay-stat', text: `avg ${duration(lat.averageDurationMs)} · p95 ${duration(lat.p95DurationMs)}` })
    if (Number(lat.slaViolationCount) > 0) {
      lines.push({ cls: 'overlay-sla', text: `SLA breaches ${count(lat.slaViolationCount)}` })
    }
  } else if (itemsProcessed > 0) {
    lines.push({ cls: 'overlay-stat-muted', text: 'latency pending' })
  } else {
    lines.push({ cls: 'overlay-stat', text: 'no completed items yet' })
  }

  if (alarm) {
    lines.push({ cls: 'overlay-alarm', text: `${alarm.severity} alarm ×${count(alarm.fireCount || 1)}` })
    if (alarm.lastMessage) {
      lines.push({ cls: 'overlay-alarm-msg', text: alarm.lastMessage })
    }
  }

  return { cls, label: id, lines }
}

export function renderOverlayHtml(model) {
  if (!model) return ''
  let html = `<div class="bpmn-overlay ${model.cls}"><span class="overlay-label">${escapeHtml(model.label)}</span>`
  for (const line of model.lines) {
    html += `<span class="${line.cls}">${escapeHtml(line.text)}</span>`
  }
  html += '</div>'
  return html
}

// Index an alarm list by activity id, keeping the most serious alarm per activity.
export function alarmsByActivity(alarms) {
  const map = new Map()
  for (const alarm of alarms || []) {
    if (alarm.activityId) {
      map.set(alarm.activityId, strongerAlarm(map.get(alarm.activityId), alarm))
    }
  }
  return map
}

// Index a latency summary list by activity id.
export function latencyByActivity(latency) {
  const map = new Map()
  for (const row of latency || []) {
    if (row.activityId) map.set(row.activityId, row)
  }
  return map
}

// Index a throughput list by activity id, mapping to the processed count.
export function throughputByActivity(throughput) {
  const map = new Map()
  for (const row of throughput || []) {
    if (row.activityId) map.set(row.activityId, Number(row.count || 0))
  }
  return map
}
