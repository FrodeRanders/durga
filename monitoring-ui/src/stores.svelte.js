import {
  dashboardRequestPaths,
  diagramRequestPath,
  instanceRequestPath,
  normalizeDashboardResponses,
  processRequestPath
} from './api.js'

let processId = $state('invoice_receipt')
let threshold = $state(60)
let refreshSecs = $state(3)
let instanceId = $state('')

let health = $state({ streamsState: '...' })
let allCounts = $state([])
let counts = $state([])
let latency = $state([])
let stuck = $state([])
let trends = $state([])
let instanceView = $state(null)
let diagramAvailable = $state(false)
let error = $state(null)

let interval = null

async function safeFetchJson(path) {
  try {
    const res = await fetch(path)
    if (!res.ok) {
      console.log('[durga] fetch %s → %d', path, res.status)
      return null
    }
    const body = await res.json()
    return { status: res.status, body }
  } catch (e) {
    console.log('[durga] fetch %s → err %s', path, e.message)
    return null
  }
}

export async function refresh() {
  error = null
  const paths = dashboardRequestPaths(processId, threshold)
  console.log('[durga] refresh pid=%s paths=%o', processId, paths)
  const responses = await Promise.all(paths.map(safeFetchJson))
  const normalized = normalizeDashboardResponses(responses)
  health = normalized.health
  allCounts = normalized.allCounts
  counts = normalized.counts
  latency = normalized.latency
  stuck = normalized.stuck
  trends = normalized.trends
  console.log('[durga] refresh done health=%o counts=%d latency=%d stuck=%d trends=%d',
    health?.streamsState, counts.length, latency.length, stuck.length, trends.length)
}

export async function refreshInstance() {
  if (!instanceId) {
    instanceView = null
    return
  }
  try {
    const res = await fetch(`/api/instances/${encodeURIComponent(instanceId)}`)
    if (!res.ok) {
      instanceView = { error: `HTTP ${res.status}` }
      return
    }
    instanceView = await res.json()
  } catch (e) {
    instanceView = { error: e.message }
  }
}

export async function discoverProcessId() {
  try {
    const res = await fetch(processRequestPath())
    if (res.ok) {
      const data = await res.json()
      if (data.processId && data.processId.length > 0) {
        processId = data.processId
        console.log('[durga] discovered pid=%s from /api/process', processId)
        return
      }
    }
  } catch {
    // try fallback
  }
  // Fallback: use first processId from /api/counts
  try {
    const res = await fetch('/api/counts')
    if (res.ok) {
      const data = await res.json()
      if (Array.isArray(data) && data.length > 0 && data[0].processId) {
        processId = data[0].processId
        console.log('[durga] discovered pid=%s from /api/counts', processId)
        return
      }
    }
  } catch {
    // keep default
  }
  console.log('[durga] using default pid=%s', processId)
}

export async function checkDiagramAvailable() {
  try {
    const res = await fetch(diagramRequestPath())
    diagramAvailable = res.ok
  } catch {
    diagramAvailable = false
  }
}

export function scheduleRefresh(getRefresh) {
  if (interval) clearInterval(interval)
  getRefresh()
  interval = setInterval(getRefresh, Math.max(1, refreshSecs) * 1000)
  console.log('[durga] polling started every %ds', refreshSecs)
}

export function getState() {
  return {
    get processId() { return processId },
    set processId(v) { processId = v },
    get threshold() { return threshold },
    set threshold(v) { threshold = v },
    get refreshSecs() { return refreshSecs },
    set refreshSecs(v) { refreshSecs = v },
    get instanceId() { return instanceId },
    set instanceId(v) { instanceId = v },
    get health() { return health },
    get allCounts() { return allCounts },
    get counts() { return counts },
    get latency() { return latency },
    get stuck() { return stuck },
    get trends() { return trends },
    get instanceView() { return instanceView },
    get diagramAvailable() { return diagramAvailable },
    get error() { return error },
    refresh,
    refreshInstance,
    discoverProcessId,
    checkDiagramAvailable,
    scheduleRefresh
  }
}
