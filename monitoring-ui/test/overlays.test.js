import assert from 'node:assert/strict'
import test from 'node:test'

import {
  buildTaskOverlay,
  renderOverlayHtml,
  strongerAlarm,
  alarmsByActivity,
  latencyByActivity,
  throughputByActivity
} from '../src/overlays.js'

const latency = {
  activityId: 'transform',
  sampleCount: 1234,
  averageDurationMs: 800,
  p95DurationMs: 6200,
  slaViolationCount: 0
}

test('buildTaskOverlay shows throughput and latency, not individual executions', () => {
  const model = buildTaskOverlay('transform', 1234, latency, null)
  assert.equal(model.label, 'transform')
  assert.equal(model.cls, 'overlay-slow') // p95 > 5s
  const texts = model.lines.map((l) => l.text)
  assert.ok(texts.some((t) => t.includes('processed') && t.includes('1,234')))
  assert.ok(texts.some((t) => t.includes('avg') && t.includes('p95')))
})

test('throughput alone (no latency) still shows items processed, latency pending', () => {
  const model = buildTaskOverlay('transform', 5468, null, null)
  assert.equal(model.cls, 'overlay-ok')
  const texts = model.lines.map((l) => l.text)
  assert.ok(texts.some((t) => t.includes('processed') && t.includes('5,468')))
  assert.ok(model.lines.some((l) => l.cls === 'overlay-stat-muted' && l.text.includes('latency pending')))
})

test('buildTaskOverlay falls back to latency sampleCount when no throughput given', () => {
  const model = buildTaskOverlay('transform', undefined, latency, null)
  assert.ok(model.lines.some((l) => l.text.includes('processed') && l.text.includes('1,234')))
})

test('buildTaskOverlay flags SLA breaches as critical', () => {
  const model = buildTaskOverlay('transform', 1234, { ...latency, slaViolationCount: 3 }, null)
  assert.equal(model.cls, 'overlay-danger')
  assert.ok(model.lines.some((l) => l.cls === 'overlay-sla' && l.text.includes('3')))
})

test('an alarm is layered on top of statistics (additive) and drives the colour', () => {
  const alarm = { severity: 'CRITICAL', fireCount: 5, lastMessage: 'backlog growing' }
  const model = buildTaskOverlay('transform', 1234, latency, alarm)
  assert.equal(model.cls, 'overlay-danger') // alarm outranks latency severity
  const texts = model.lines.map((l) => l.text)
  assert.ok(texts.some((t) => t.includes('processed'))) // stats retained
  assert.ok(texts.some((t) => t.includes('CRITICAL alarm') && t.includes('5')))
  assert.ok(texts.some((t) => t.includes('backlog growing')))
})

test('a task with an alarm but no throughput/latency still renders the alarm', () => {
  const model = buildTaskOverlay('transform', 0, null, { severity: 'WARNING', fireCount: 1 })
  assert.equal(model.cls, 'overlay-warn')
  assert.ok(model.lines.some((l) => l.text.includes('no completed items yet')))
  assert.ok(model.lines.some((l) => l.text.includes('WARNING alarm')))
})

test('buildTaskOverlay returns null when there is nothing to show', () => {
  assert.equal(buildTaskOverlay('idle', 0, null, null), null)
  assert.equal(buildTaskOverlay('idle', undefined, { activityId: 'idle', sampleCount: 0 }, null), null)
})

test('renderOverlayHtml escapes content and applies line classes', () => {
  const html = renderOverlayHtml(buildTaskOverlay('t', 1234, latency, {
    severity: 'CRITICAL',
    fireCount: 1,
    lastMessage: '<script>bad</script>'
  }))
  assert.ok(html.includes('class="bpmn-overlay overlay-danger"'))
  assert.ok(html.includes('overlay-alarm-msg'))
  assert.ok(html.includes('&lt;script&gt;'))
  assert.ok(!html.includes('<script>bad'))
})

test('strongerAlarm prefers CRITICAL then most recent', () => {
  const warn = { severity: 'WARNING', lastTriggeredAt: '2026-01-01T00:00:00Z' }
  const crit = { severity: 'CRITICAL', lastTriggeredAt: '2025-01-01T00:00:00Z' }
  assert.equal(strongerAlarm(warn, crit), crit)
  const older = { severity: 'WARNING', lastTriggeredAt: '2026-01-01T00:00:00Z' }
  const newer = { severity: 'WARNING', lastTriggeredAt: '2026-06-01T00:00:00Z' }
  assert.equal(strongerAlarm(older, newer), newer)
})

test('index helpers key by activity id', () => {
  const alarms = [
    { activityId: 'a', severity: 'WARNING', lastTriggeredAt: '2026-01-01T00:00:00Z' },
    { activityId: 'a', severity: 'CRITICAL', lastTriggeredAt: '2025-01-01T00:00:00Z' },
    { severity: 'WARNING' } // no activity id — ignored
  ]
  const byActivity = alarmsByActivity(alarms)
  assert.equal(byActivity.size, 1)
  assert.equal(byActivity.get('a').severity, 'CRITICAL')

  const latMap = latencyByActivity([latency])
  assert.equal(latMap.get('transform').sampleCount, 1234)

  const tpMap = throughputByActivity([{ processId: 'p', activityId: 'transform', count: 42 }])
  assert.equal(tpMap.get('transform'), 42)
})
