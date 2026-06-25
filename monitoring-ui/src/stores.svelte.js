import {
  dashboardRequestPaths,
  diagramRequestPath,
  instanceRequestPath,
  normalizeDashboardResponses,
  processRequestPath
} from './api.js'

let processId = $state('')
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

async function fetchJson(path) {
  const res = await fetch(path)
  return { status: res.status, body: await res.json() }
}

export async function refresh() {
  error = null
  try {
    const responses = await Promise.all(
      dashboardRequestPaths(processId, threshold).map(fetchJson)
    )
    const normalized = normalizeDashboardResponses(responses)
    health = normalized.health
    allCounts = normalized.allCounts
    counts = normalized.counts
    latency = normalized.latency
    stuck = normalized.stuck
    trends = normalized.trends
  } catch (e) {
    error = e.message
  }
}

export async function refreshInstance() {
  if (!instanceId) {
    instanceView = null
    return
  }
  try {
    const res = await fetchJson(instanceRequestPath(instanceId))
    instanceView = res.body
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
