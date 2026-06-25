export function dashboardRequestPaths(processId, thresholdSeconds) {
  const pid = encodeURIComponent(processId ?? '')
  const age = encodeURIComponent(thresholdSeconds ?? '')
  return [
    '/api/health',
    '/api/counts',
    `/api/processes/${pid}/counts`,
    `/api/processes/${pid}/latency`,
    `/api/stuck?processId=${pid}&olderThanSeconds=${age}`,
    `/api/processes/${pid}/trends`
  ]
}

export function diagramRequestPath() {
  return '/api/diagram'
}

export function instanceRequestPath(instanceId) {
  if (!instanceId) {
    return null
  }
  return `/api/instances/${encodeURIComponent(instanceId)}`
}

export function normalizeDashboardResponses(responses) {
  const [health, allCounts, counts, latency, stuck, trends] = responses
  return {
    health: health?.body ?? { streamsState: '...' },
    allCounts: Array.isArray(allCounts?.body) ? allCounts.body : [],
    counts: Array.isArray(counts?.body) ? counts.body : [],
    latency: Array.isArray(latency?.body) ? latency.body : [],
    stuck: Array.isArray(stuck?.body) ? stuck.body : [],
    trends: Array.isArray(trends?.body) ? trends.body : []
  }
}
