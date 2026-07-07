import {
  dashboardRequestPaths,
  diagramRequestPath,
  instanceAlarmsRequestPath,
  instanceRequestPath,
  normalizeDashboardResponses,
  processListRequestPath,
  validationResultsRequestPath,
  validationSummaryRequestPath
} from './api.js'

let processId = $state('')
let threshold = $state(60)
let refreshSecs = $state(3)
let instanceId = $state('')

let health = $state({ streamsState: '...' })
let allCounts = $state([])
let allAlarms = $state([])
let counts = $state([])
let alarms = $state([])
let latency = $state([])
let stuck = $state([])
let trends = $state([])
let throughput = $state([])
let instanceView = $state(null)
let instanceAlarms = $state([])
let diagramAvailable = $state(false)
let error = $state(null)
let processList = $state([])

let validationSummary = $state([])
let validationResults = $state([])
let validationTask = $state('')

let interval = null

async function safeFetchJson(path) {
  try {
    const res = await fetch(path)
    if (!res.ok) return null
    const body = await res.json()
    return { status: res.status, body }
  } catch {
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
  allAlarms = normalized.allAlarms
  counts = normalized.counts
  alarms = normalized.alarms
  latency = normalized.latency
  stuck = normalized.stuck
  trends = normalized.trends
  throughput = normalized.throughput
}

export async function refreshInstance() {
  if (!instanceId) {
    instanceView = null
    instanceAlarms = []
    return
  }
  try {
    const [viewRes, alarmsRes] = await Promise.all([
      fetch(instanceRequestPath(instanceId)),
      fetch(instanceAlarmsRequestPath(instanceId))
    ])
    instanceView = viewRes.ok ? await viewRes.json() : { error: `HTTP ${viewRes.status}` }
    instanceAlarms = alarmsRes.ok ? await alarmsRes.json() : []
  } catch (e) {
    instanceView = { error: e.message }
    instanceAlarms = []
  }
}

export async function fetchProcessList() {
  try {
    const res = await fetch(processListRequestPath())
    if (res.ok) {
      processList = await res.json()
    }
  } catch {
    processList = []
  }
}

export async function checkDiagramAvailable() {
  try {
    const res = await fetch(diagramRequestPath(processId))
    diagramAvailable = res.ok
  } catch {
    diagramAvailable = false
  }
}

export async function refreshValidation() {
  if (!processId) {
    validationSummary = []
    validationResults = []
    return
  }
  const summary = await safeFetchJson(validationSummaryRequestPath(processId))
  validationSummary = Array.isArray(summary?.body) ? summary.body : []
  const results = await safeFetchJson(
    validationResultsRequestPath(processId, validationTask || null, null))
  validationResults = Array.isArray(results?.body) ? results.body : []
}

export function selectValidationTask(taskId) {
  validationTask = validationTask === taskId ? '' : taskId
  refreshValidation()
}

export function clearValidationTask() {
  validationTask = ''
}

export function scheduleRefresh(getRefresh) {
  if (interval) clearInterval(interval)
  getRefresh()
  interval = setInterval(getRefresh, Math.max(1, refreshSecs) * 1000)
}

// Direct mutation — bypasses the broken Svelte 5 getter/setter for instanceId
export function lookupInstance(id) {
  instanceId = id
  refreshInstance()
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
    get allAlarms() { return allAlarms },
    get counts() { return counts },
    get alarms() { return alarms },
    get latency() { return latency },
    get stuck() { return stuck },
    get trends() { return trends },
    get throughput() { return throughput },
    get instanceView() { return instanceView },
    get instanceAlarms() { return instanceAlarms },
    get diagramAvailable() { return diagramAvailable },
    get error() { return error },
    get processList() { return processList },
    get validationSummary() { return validationSummary },
    get validationResults() { return validationResults },
    get validationTask() { return validationTask },
    refresh,
    refreshInstance,
    scheduleRefresh,
    lookupInstance,
    fetchProcessList,
    refreshValidation,
    selectValidationTask,
    clearValidationTask
  }
}
