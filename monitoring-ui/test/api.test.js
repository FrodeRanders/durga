import assert from 'node:assert/strict'
import test from 'node:test'

import {
  dashboardRequestPaths,
  diagramRequestPath,
  instanceRequestPath,
  normalizeDashboardResponses
} from '../src/api.js'

test('dashboardRequestPaths URL-encodes process id and threshold', () => {
  assert.deepEqual(dashboardRequestPaths('invoice review/2026', 30), [
    '/api/health',
    '/api/counts',
    '/api/processes/invoice%20review%2F2026/counts',
    '/api/processes/invoice%20review%2F2026/latency',
    '/api/stuck?processId=invoice%20review%2F2026&olderThanSeconds=30',
    '/api/processes/invoice%20review%2F2026/trends'
  ])
})

test('diagramRequestPath returns the diagram endpoint', () => {
  assert.equal(diagramRequestPath(), '/api/diagram')
})

test('instanceRequestPath returns null for empty input and encodes ids', () => {
  assert.equal(instanceRequestPath(''), null)
  assert.equal(instanceRequestPath('instance/1'), '/api/instances/instance%2F1')
})

test('normalizeDashboardResponses defaults missing collections to arrays', () => {
  const normalized = normalizeDashboardResponses([
    { body: { streamsState: 'RUNNING' } },
    { body: [{ processId: 'invoice', state: 'active', count: 2 }] },
    { body: null },
    { body: [{ activityId: 'review', sampleCount: 1 }] },
    {},
    { body: 'not an array' }
  ])

  assert.deepEqual(normalized, {
    health: { streamsState: 'RUNNING' },
    allCounts: [{ processId: 'invoice', state: 'active', count: 2 }],
    counts: [],
    latency: [{ activityId: 'review', sampleCount: 1 }],
    stuck: [],
    trends: []
  })
})
