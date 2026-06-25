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
    if (!res.ok) return null
    const body = await res.json()
    return { status: res.status, body }
  } catch (e) {
    return null
  }
}

export async function refresh() {
  error = null
  const paths = dashboardRequestPaths(processId, threshold)
  const responses = await Promise.all(paths.map(safeFetchJson))
  const normalized = normalizeDashboardResponses(responses)
  health = normalized.health
  allCounts = normalized.allCounts
  counts = normalized.counts
  latency = normalized.latency
  stuck = normalized.stuck
  trends = normalized.trends
}

export async function refreshInstance() {
  if (!instanceId) {
    instanceView = null
    return
  }
  try {
    const url = `/api/instances/${encodeURIComponent(instanceId)}`
    const res = await fetch(url)
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
        return
      }
    }
  } catch {
    // try fallback
  }
  try {
    const res = await fetch('/api/counts')
    if (res.ok) {
      const data = await res.json()
      if (Array.isArray(data) && data.length > 0 && data[0].processId) {
        processId = data[0].processId
      }
    }
  } catch {
    // keep default
  }
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
}

export function selectAndFetchInstance(id) {
  instanceId = id
  refreshInstance()
}

export function selectProcessId(pid) {
  processId = pid
}

export function getState() {
  return {
    get processId() { return processId },
    get threshold() { return threshold },
    get refreshSecs() { return refreshSecs },
    get instanceId() { return instanceId },
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
    scheduleRefresh,
    selectAndFetchInstance,
    selectProcessId
  }
}
