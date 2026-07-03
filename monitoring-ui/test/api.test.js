import assert from 'node:assert/strict'
import test from 'node:test'

import {
  dashboardRequestPaths,
  diagramRequestPath,
  instanceAlarmsRequestPath,
  instanceRequestPath,
  normalizeDashboardResponses,
  processListRequestPath
} from '../src/api.js'

test('dashboardRequestPaths URL-encodes process id and threshold', () => {
  assert.deepEqual(dashboardRequestPaths('invoice review/2026', 30), [
    '/health',
    '/api/counts',
    '/api/alarms',
    '/api/processes/invoice%20review%2F2026/counts',
    '/api/processes/invoice%20review%2F2026/alarms',
    '/api/processes/invoice%20review%2F2026/latency',
    '/api/stuck?processId=invoice%20review%2F2026&olderThanSeconds=30',
    '/api/processes/invoice%20review%2F2026/trends'
  ])
})

test('processListRequestPath returns the process inventory endpoint', () => {
  assert.equal(processListRequestPath(), '/api/processes/list')
})

test('diagramRequestPath returns endpoint with optional processId', () => {
  assert.equal(diagramRequestPath(), '/api/diagram')
  assert.equal(diagramRequestPath(''), '/api/diagram')
  assert.equal(diagramRequestPath('invoice'), '/api/diagram?processId=invoice')
  assert.equal(diagramRequestPath('order/fulfill'), '/api/diagram?processId=order%2Ffulfill')
})

test('instanceRequestPath returns null for empty input and encodes ids', () => {
  assert.equal(instanceRequestPath(''), null)
  assert.equal(instanceRequestPath('instance/1'), '/api/instances/instance%2F1')
})

test('instanceAlarmsRequestPath returns null for empty input and encodes ids', () => {
  assert.equal(instanceAlarmsRequestPath(''), null)
  assert.equal(instanceAlarmsRequestPath('instance/1'), '/api/instances/instance%2F1/alarms')
})

test('normalizeDashboardResponses defaults missing collections to arrays', () => {
  const normalized = normalizeDashboardResponses([
    { body: { streamsState: 'RUNNING' } },
    { body: [{ processId: 'invoice', state: 'active', count: 2 }] },
    { body: [{ processId: 'invoice', severity: 'WARN', fireCount: 1 }] },
    { body: null },
    { body: [{ processId: 'invoice', severity: 'CRITICAL', fireCount: 2 }] },
    { body: [{ activityId: 'review', sampleCount: 1 }] },
    {},
    { body: 'not an array' }
  ])

  assert.deepEqual(normalized, {
    health: { streamsState: 'RUNNING' },
    allCounts: [{ processId: 'invoice', state: 'active', count: 2 }],
    allAlarms: [{ processId: 'invoice', severity: 'WARN', fireCount: 1 }],
    counts: [],
    alarms: [{ processId: 'invoice', severity: 'CRITICAL', fireCount: 2 }],
    latency: [{ activityId: 'review', sampleCount: 1 }],
    stuck: [],
    trends: []
  })
})
