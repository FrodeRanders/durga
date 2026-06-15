package org.gautelis.durga.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Small embedded HTTP surface for the monitoring topology.
 * <p>
 * The server exposes health, instance lookup, aggregate queries, metrics,
 * and the embedded dashboard without introducing a second application framework.
 */
public final class ProcessMonitoringHttpServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitoringHttpServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaStreams streams;
    private final ProcessMonitoringQueryService queryService;
    private final HttpServer server;
    private final String apiKey;

    /**
     * Creates the HTTP server bound to an existing monitoring topology.
     *
     * @param streams running Kafka Streams instance
     * @param topics monitoring topic and store names
     * @param port local HTTP port
     * @throws IOException if the embedded server cannot be created
     */
    public ProcessMonitoringHttpServer(
            KafkaStreams streams,
            ProcessMonitoringTopology.MonitoringTopics topics,
            int port
    ) throws IOException {
        this.streams = streams;
        this.queryService = new ProcessMonitoringQueryService(streams, topics);
        this.apiKey = resolveApiKey();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/", this::handleRoot);
        this.server.createContext("/dashboard", this::handleDashboard);
        this.server.createContext("/health", this::handleHealth);
        this.server.createContext("/instances", this::handleInstances);
        this.server.createContext("/processes", this::handleProcesses);
        this.server.createContext("/counts", this::handleCounts);
        this.server.createContext("/stuck", this::handleStuck);
        this.server.createContext("/metrics", this::handleMetrics);
        LOG.info("Monitoring HTTP server created on port {}", port);
    }

    /**
     * Starts serving HTTP requests.
     */
    public void start() {
        server.start();
        LOG.info("Monitoring HTTP server started");
    }

    /**
     * Stops the embedded HTTP server immediately.
     */
    @Override
    public void close() {
        server.stop(0);
        LOG.info("Monitoring HTTP server stopped");
    }

    private boolean requireAuth(HttpExchange exchange) throws IOException {
        if (apiKey == null) {
            return true;
        }
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        String expected = "Bearer " + apiKey;
        if (auth == null || !auth.equals(expected)) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
            sendJson(exchange, 401, Map.of("error", "unauthorized"));
            return false;
        }
        return true;
    }

    private void handleRoot(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        String path = exchange.getRequestURI().getPath();
        if ("/".equals(path)) {
            redirect(exchange, "/dashboard");
            return;
        }
        sendStatus(exchange, 404);
    }

    private void handleDashboard(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        sendHtml(exchange, 200, dashboardHtml());
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        sendJson(exchange, 200, Map.of("streamsState", streams.state().name()));
    }

    private void handleInstances(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        List<String> parts = pathParts(exchange.getRequestURI().getPath());
        if (parts.size() != 2) {
            sendJson(exchange, 400, Map.of("error", "Expected /instances/{processInstanceId}"));
            return;
        }

        try {
            Optional<ProcessStateView> state = queryService.findInstance(parts.get(1));
            if (state.isEmpty()) {
                sendJson(exchange, 404, Map.of("error", "Process instance not found"));
                return;
            }
            sendJson(exchange, 200, state.get());
        } catch (InvalidStateStoreException e) {
            sendJson(exchange, 503, Map.of("error", "State store not queryable yet"));
        }
    }

    private void handleProcesses(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        List<String> parts = pathParts(exchange.getRequestURI().getPath());
        if (parts.size() != 3) {
            sendJson(exchange, 400, Map.of("error", "Expected /processes/{processId}/counts, /latency, or /trends"));
            return;
        }

        try {
            // Keep the HTTP surface intentionally small: both routes are just read-model views
            // over the same materialized stores, not separate service layers.
            if ("counts".equals(parts.get(2))) {
                sendJson(exchange, 200, queryService.countsForProcess(parts.get(1)));
                return;
            }
            if ("latency".equals(parts.get(2))) {
                sendJson(exchange, 200, queryService.latencyForProcess(parts.get(1)));
                return;
            }
            if ("trends".equals(parts.get(2))) {
                sendJson(exchange, 200, queryService.trendsForProcess(parts.get(1)));
                return;
            }
            sendJson(exchange, 400, Map.of("error", "Expected /processes/{processId}/counts, /latency, or /trends"));
        } catch (InvalidStateStoreException e) {
            sendJson(exchange, 503, Map.of("error", "State store not queryable yet"));
        }
    }

    private void handleCounts(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        try {
            sendJson(exchange, 200, queryService.allCounts());
        } catch (InvalidStateStoreException e) {
            sendJson(exchange, 503, Map.of("error", "State store not queryable yet"));
        }
    }

    private void handleStuck(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        if (!requireAuth(exchange)) return;
        try {
            // "Stuck" is query-time configurable, so the threshold remains an HTTP parameter
            // instead of being baked into the monitoring topology itself.
            Map<String, String> query = queryParams(exchange.getRequestURI().getRawQuery());
            String processId = query.get("processId");
            long olderThanSeconds = parseLong(query.getOrDefault("olderThanSeconds", "60"), 60L);
            sendJson(exchange, 200, queryService.stuckInstances(processId, olderThanSeconds));
        } catch (InvalidStateStoreException e) {
            sendJson(exchange, 503, Map.of("error", "State store not queryable yet"));
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendStatus(exchange, 405);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        var body = org.gautelis.durga.monitoring.Metrics.scrape().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static String resolveApiKey() {
        String key = System.getProperty("durga.monitoring.api.key");
        if (key == null) {
            key = System.getenv("DURGA_MONITORING_API_KEY");
        }
        if (key != null && key.isBlank()) {
            key = null;
        }
        if (key == null) {
            LOG.warn("No DURGA_MONITORING_API_KEY set — monitoring API is unauthenticated");
        }
        return key;
    }

    private static List<String> pathParts(String path) {
        return List.of(path.split("/")).stream()
                .filter(part -> !part.isBlank())
                .toList();
    }

    private static Map<String, String> queryParams(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }
        Map<String, String> result = new java.util.LinkedHashMap<>();
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            if (parts.length == 2) {
                result.put(parts[0], parts[1]);
            }
        }
        return result;
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static void sendStatus(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static void redirect(HttpExchange exchange, String location) throws IOException {
        exchange.getResponseHeaders().set("Location", location);
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    private static void sendJson(HttpExchange exchange, int status, Object payload) throws IOException {
        byte[] body = MAPPER.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static void sendHtml(HttpExchange exchange, int status, String html) throws IOException {
        byte[] body = html.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static String dashboardHtml() {
        // The dashboard is embedded on purpose: it stays in lockstep with the HTTP API and
        // avoids a separate frontend build for what is primarily a local operator view.
        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>Durga Monitor</title>
                  <style>
                    :root {
                      --bg: #f4efe6;
                      --panel: #fffaf0;
                      --ink: #1f1a17;
                      --muted: #6b6258;
                      --line: #d8c7ae;
                      --accent: #b4542f;
                      --accent-2: #2f6c63;
                    }
                    * { box-sizing: border-box; }
                    body {
                      margin: 0;
                      font-family: Georgia, "Iowan Old Style", serif;
                      color: var(--ink);
                      background:
                        radial-gradient(circle at top left, #f7d7b2 0, transparent 28rem),
                        radial-gradient(circle at bottom right, #d8eadf 0, transparent 30rem),
                        var(--bg);
                    }
                    main {
                      max-width: 1100px;
                      margin: 0 auto;
                      padding: 32px 18px 60px;
                    }
                    h1, h2 {
                      margin: 0;
                      font-weight: 700;
                      letter-spacing: 0.02em;
                    }
                    p { color: var(--muted); }
                    .hero {
                      display: grid;
                      gap: 14px;
                      margin-bottom: 22px;
                    }
                    .hero h1 {
                      font-size: clamp(2.4rem, 6vw, 4.6rem);
                      line-height: 0.95;
                    }
                    .controls, .grid {
                      display: grid;
                      gap: 16px;
                    }
                    .controls {
                      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                      margin-bottom: 18px;
                    }
                    label {
                      display: grid;
                      gap: 6px;
                      font-size: 0.92rem;
                      color: var(--muted);
                    }
                    input {
                      width: 100%;
                      padding: 12px 14px;
                      border: 1px solid var(--line);
                      border-radius: 14px;
                      background: rgba(255,255,255,0.9);
                      color: var(--ink);
                    }
                    .grid {
                      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                    }
                    .panel {
                      background: rgba(255,250,240,0.88);
                      border: 1px solid var(--line);
                      border-radius: 22px;
                      padding: 18px;
                      box-shadow: 0 10px 30px rgba(70, 44, 24, 0.08);
                      backdrop-filter: blur(6px);
                    }
                    .stat {
                      font-size: 2rem;
                      color: var(--accent);
                      margin-top: 8px;
                    }
                    table {
                      width: 100%;
                      border-collapse: collapse;
                      font-size: 0.92rem;
                    }
                    th, td {
                      text-align: left;
                      padding: 10px 8px;
                      border-bottom: 1px solid var(--line);
                      vertical-align: top;
                    }
                    th { color: var(--muted); font-weight: 600; }
                    .pill {
                      display: inline-block;
                      padding: 4px 10px;
                      border-radius: 999px;
                      background: #efe1cf;
                      color: var(--accent-2);
                      font-size: 0.82rem;
                    }
                    pre {
                      margin: 0;
                      white-space: pre-wrap;
                      word-break: break-word;
                      font-size: 0.88rem;
                    }
                    .muted { color: var(--muted); }
                  </style>
                </head>
                <body>
                <main>
                  <section class="hero">
                    <div class="pill">Durga Monitoring Dashboard</div>
                    <h1>Process traffic, state, and lag in one place.</h1>
                    <p>Live view backed by the Kafka Streams query stores exposed by this monitoring app.</p>
                  </section>

                  <section class="controls">
                    <label>
                      Process ID
                      <input id="processId" value="invoice_receipt">
                    </label>
                    <label>
                      Older Than Seconds
                      <input id="olderThanSeconds" type="number" min="1" value="60">
                    </label>
                    <label>
                      Refresh Interval Seconds
                      <input id="refreshSeconds" type="number" min="1" value="3">
                    </label>
                  </section>

                  <section class="grid">
                    <article class="panel">
                      <h2>Health</h2>
                      <div class="stat" id="streamsState">...</div>
                    </article>
                    <article class="panel">
                      <h2>Counts</h2>
                      <table id="countsTable"></table>
                    </article>
                    <article class="panel">
                      <h2>Latency</h2>
                      <table id="latencyTable"></table>
                    </article>
                    <article class="panel">
                      <h2>Stuck Instances</h2>
                      <table id="stuckTable"></table>
                    </article>
                    <article class="panel" style="grid-column: 1 / -1;">
                      <h2>Selected Instance</h2>
                      <p class="muted">Paste a process instance id from the stuck table or your logs.</p>
                      <label>
                        Process Instance ID
                        <input id="instanceId" placeholder="paste a processInstanceId">
                      </label>
                      <pre id="instanceView" class="muted">No instance selected.</pre>
                    </article>
                  </section>
                </main>
                <script>
                  const qs = (id) => document.getElementById(id);
                  const processId = qs('processId');
                  const olderThanSeconds = qs('olderThanSeconds');
                  const refreshSeconds = qs('refreshSeconds');
                  const instanceId = qs('instanceId');
                  const streamsState = qs('streamsState');
                  const countsTable = qs('countsTable');
                  const latencyTable = qs('latencyTable');
                  const stuckTable = qs('stuckTable');
                  const instanceView = qs('instanceView');
                  let timer = null;

                  async function fetchJson(path) {
                    const response = await fetch(path);
                    return { status: response.status, body: await response.json() };
                  }

                  function renderTable(target, headers, rows) {
                    const thead = `<tr>${headers.map((h) => `<th>${h}</th>`).join('')}</tr>`;
                    const tbody = rows.length === 0
                      ? `<tr><td colspan="${headers.length}" class="muted">No data</td></tr>`
                      : rows.map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join('')}</tr>`).join('');
                    target.innerHTML = thead + tbody;
                  }

                  async function refreshDashboard() {
                    const pid = encodeURIComponent(processId.value.trim());
                    const age = encodeURIComponent(olderThanSeconds.value.trim() || '60');

                    const health = await fetchJson('/health');
                    streamsState.textContent = health.body.streamsState ?? 'UNKNOWN';

                    const counts = await fetchJson(`/processes/${pid}/counts`);
                    renderTable(countsTable, ['State', 'Count'], (counts.body || []).map((row) => [row.state, row.count]));

                    const latency = await fetchJson(`/processes/${pid}/latency`);
                    renderTable(latencyTable, ['Activity', 'Samples', 'Avg ms', 'Max ms'],
                      (latency.body || []).map((row) => [row.activityId, row.sampleCount, row.averageDurationMs, row.maxDurationMs]));

                    const stuck = await fetchJson(`/stuck?processId=${pid}&olderThanSeconds=${age}`);
                    renderTable(stuckTable, ['Instance', 'Activity', 'Age s', 'State'],
                      (stuck.body || []).map((row) => [
                        `<button type="button" data-instance="${row.processInstanceId}">${row.processInstanceId.slice(0, 8)}</button>`,
                        row.currentActivityId ?? '',
                        row.ageSeconds,
                        row.lifecycleState
                      ]));

                    for (const button of stuckTable.querySelectorAll('button[data-instance]')) {
                      button.addEventListener('click', () => {
                        instanceId.value = button.dataset.instance;
                        refreshInstance();
                      });
                    }
                  }

                  async function refreshInstance() {
                    const id = instanceId.value.trim();
                    if (!id) {
                      instanceView.textContent = 'No instance selected.';
                      return;
                    }
                    const result = await fetchJson(`/instances/${encodeURIComponent(id)}`);
                    instanceView.textContent = JSON.stringify(result.body, null, 2);
                  }

                  function scheduleRefresh() {
                    if (timer) clearInterval(timer);
                    refreshDashboard();
                    timer = setInterval(refreshDashboard, Math.max(1, Number(refreshSeconds.value || '3')) * 1000);
                  }

                  processId.addEventListener('change', scheduleRefresh);
                  olderThanSeconds.addEventListener('change', scheduleRefresh);
                  refreshSeconds.addEventListener('change', scheduleRefresh);
                  instanceId.addEventListener('change', refreshInstance);
                  scheduleRefresh();
                </script>
                </body>
                </html>
                """;
    }
}
