export function dashboardRequestPaths(processId, thresholdSeconds) {
  const pid = encodeURIComponent(processId ?? '')
  const age = encodeURIComponent(thresholdSeconds ?? '')
  return [
    '/api/health',
    '/api/counts',
    '/api/alarms',
    `/api/processes/${pid}/counts`,
    `/api/processes/${pid}/alarms`,
    `/api/processes/${pid}/latency`,
    `/api/stuck?processId=${pid}&olderThanSeconds=${age}`,
    `/api/processes/${pid}/trends`
  ]
}

export function processListRequestPath() {
  return '/api/processes/list'
}

export function diagramRequestPath(processId) {
  const pid = processId ? encodeURIComponent(processId) : ''
  return pid ? `/api/diagram?processId=${pid}` : '/api/diagram'
}

export function instanceRequestPath(instanceId) {
  if (!instanceId) {
    return null
  }
  return `/api/instances/${encodeURIComponent(instanceId)}`
}

export function instanceAlarmsRequestPath(instanceId) {
  if (!instanceId) {
    return null
  }
  return `/api/instances/${encodeURIComponent(instanceId)}/alarms`
}

export function normalizeDashboardResponses(responses) {
  const [health, allCounts, allAlarms, counts, alarms, latency, stuck, trends] = responses
  return {
    health: health?.body ?? { streamsState: '...' },
    allCounts: Array.isArray(allCounts?.body) ? allCounts.body : [],
    allAlarms: Array.isArray(allAlarms?.body) ? allAlarms.body : [],
    counts: Array.isArray(counts?.body) ? counts.body : [],
    alarms: Array.isArray(alarms?.body) ? alarms.body : [],
    latency: Array.isArray(latency?.body) ? latency.body : [],
    stuck: Array.isArray(stuck?.body) ? stuck.body : [],
    trends: Array.isArray(trends?.body) ? trends.body : []
  }
}
