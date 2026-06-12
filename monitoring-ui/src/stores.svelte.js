let processId = $state('invoice_receipt')
let threshold = $state(60)
let refreshSecs = $state(3)
let instanceId = $state('')

let health = $state({ streamsState: '...' })
let counts = $state([])
let latency = $state([])
let stuck = $state([])
let trends = $state([])
let instanceView = $state(null)
let error = $state(null)

let interval = null

async function fetchJson(path) {
  const res = await fetch(path)
  return { status: res.status, body: await res.json() }
}

export async function refresh() {
  error = null
  try {
    const pid = encodeURIComponent(processId)
    const age = encodeURIComponent(threshold)
    const [h, c, l, s, t] = await Promise.all([
      fetchJson('/api/health'),
      fetchJson(`/api/processes/${pid}/counts`),
      fetchJson(`/api/processes/${pid}/latency`),
      fetchJson(`/api/stuck?processId=${pid}&olderThanSeconds=${age}`),
      fetchJson(`/api/processes/${pid}/trends`)
    ])
    health = h.body
    counts = c.body || []
    latency = l.body || []
    stuck = s.body || []
    trends = t.body || []
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
    const res = await fetchJson(`/api/instances/${encodeURIComponent(instanceId)}`)
    instanceView = res.body
  } catch (e) {
    instanceView = { error: e.message }
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
    get counts() { return counts },
    get latency() { return latency },
    get stuck() { return stuck },
    get trends() { return trends },
    get instanceView() { return instanceView },
    get error() { return error },
    refresh,
    refreshInstance,
    scheduleRefresh
  }
}
