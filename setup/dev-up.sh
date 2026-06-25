#!/usr/bin/env bash
set -euo pipefail

# ── dev-up.sh ───────────────────────────────────────────────────────────────
# One-command full-stack demo: Kafka, monitoring backends, Svelte SPA, and
# process feed generators.  Configure via env vars or edit the defaults below.
#
# Usage:
#   ./setup/dev-up.sh
#
#   # Two processes side by side:
#   PROCESSES="invoice_receipt:src/test/resources/bpmn/invoice_receipt.bpmn,order_fulfillment:src/test/resources/bpmn/order_fulfillment.bpmn" \
#     ./setup/dev-up.sh
#
#   # Don't start Docker Kafka (assume already running):
#   START_KAFKA=false ./setup/dev-up.sh
#
# Env vars:
#   PROCESSES      comma-separated "pid:bpmnPath[:intervalMs]" entries
#   BOOTSTRAP      Kafka bootstrap servers (default localhost:9094)
#   BACKEND_PORT   base backend API port (default 8081)
#   VITE_PORT      base Vite dev-server port (default 5173)
#   START_KAFKA    auto-start Kafka via docker compose (default true)
#   SKIP_BUILD     skip Maven + npm build (default false)
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration defaults ──────────────────────────────────────────────────
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
BACKEND_PORT="${BACKEND_PORT:-8081}"
VITE_PORT="${VITE_PORT:-5173}"
START_KAFKA="${START_KAFKA:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"
PROCESSES="${PROCESSES:-invoice_receipt:src/test/resources/bpmn/invoice_receipt.bpmn:1000}"

declare -a BG_PIDS=()

_cleanup() {
    echo ""
    echo "━━━ Shutting down ━━━"
    for pid in "${BG_PIDS[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done
    sleep 2
    for pid in "${BG_PIDS[@]}"; do
        kill -9 "${pid}" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap _cleanup EXIT INT TERM

# ── Colour helpers ──────────────────────────────────────────────────────────
BOLD="\033[1m"
GREEN="\033[32m"
CYAN="\033[36m"
RESET="\033[0m"

banner()  { printf "${BOLD}━━━ %s ━━━${RESET}\n" "$1"; }
info()    { printf "  ${GREEN}→${RESET} %s\n" "$1"; }
link()    { printf "  ${CYAN}%s${RESET}\n" "$1"; }

# ── Prerequisite checks ──────────────────────────────────────────────────────
for cmd in java mvn node npm; do
    if ! command -v "${cmd}" &> /dev/null; then
        echo "ERROR: '${cmd}' is required but not found in PATH" >&2
        exit 1
    fi
done

# ── 1. Kafka ────────────────────────────────────────────────────────────────
if [[ "${START_KAFKA}" == "true" ]]; then
    banner "Starting Kafka"
    (cd "${SCRIPT_DIR}" && docker compose up -d)
    info "Waiting for Kafka broker..."
    for i in $(seq 1 30); do
        if docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka_b \
            kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    info "Kafka broker: ${BOOTSTRAP}"
    info "Kafka UI:     http://localhost:8080"
fi

# ── 2. Build ────────────────────────────────────────────────────────────────
if [[ "${SKIP_BUILD}" != "true" ]]; then
    banner "Building"
    cd "${ROOT_DIR}"
    if [[ ! -d monitoring-ui/dist ]]; then
        mkdir -p monitoring-ui/dist
        touch monitoring-ui/dist/.gitkeep
    fi
    mvn -q package -DskipTests
    (cd monitoring-ui && npm install --silent 2>/dev/null || true)
    info "Jar and SPA ready"
fi

# Resolve the JAR (handle version changes)
JAR="$(find "${ROOT_DIR}/target" -maxdepth 1 -name 'durga-*.jar' ! -name '*original*' -type f -print -quit)"
if [[ -z "${JAR}" || ! -f "${JAR}" ]]; then
    echo "ERROR: Could not find built Durga JAR under target/" >&2
    exit 1
fi

# ── 3. Clean up stale ports from previous runs ──────────────────────────────
echo ""
proc_idx=0
IFS=',' read -ra PROC_ARRAY <<< "${PROCESSES}"
for proc_def in "${PROC_ARRAY[@]}"; do
    backend_port=$((BACKEND_PORT + proc_idx))
    vite_port=$((VITE_PORT + proc_idx))
    # Kill anything holding our ports
    lsof -ti "tcp:${backend_port}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    lsof -ti "tcp:${vite_port}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    proc_idx=$((proc_idx + 1))
done

# ── 4. Parse process definitions ────────────────────────────────────────────
banner "Starting services"

proc_idx=0

for proc_def in "${PROC_ARRAY[@]}"; do
    IFS=':' read -r pid bpmn interval <<< "${proc_def}"
    interval="${interval:-1000}"
    backend_port=$((BACKEND_PORT + proc_idx))
    vite_port=$((VITE_PORT + proc_idx))
    app_id="durga-mon-${pid}"

    printf "\n  ${BOLD}%s${RESET}\n" "${pid}"
    info "API → http://localhost:${backend_port}"
    info "SPA → http://localhost:${vite_port}"
    info "Feed interval: ${interval}ms"

    # ── Monitoring backend ──────────────────────────────────────────────────
    java -Ddurga.streams.state.dir=/tmp/kafka-streams-state-${pid} \
        -cp "${JAR}" \
        org.gautelis.durga.monitoring.MonitoringContainer \
        "${BOOTSTRAP}" \
        "${app_id}" \
        "${backend_port}" \
        "${pid}" \
        "${bpmn}" \
        > /tmp/durga-backend-${pid}.log 2>&1 &
    BG_PIDS+=($!)

    # ── Continuous feed ─────────────────────────────────────────────────────
    java -cp "${JAR}" \
        org.gautelis.durga.demo.ContinuousFeedPublisher \
        "${BOOTSTRAP}" \
        "${pid}" \
        "${interval}" \
        > /tmp/durga-feed-${pid}.log 2>&1 &
    BG_PIDS+=($!)

    # ── Vite SPA dev server ─────────────────────────────────────────────────
    VITE_API_TARGET="http://localhost:${backend_port}" \
        npm run dev --prefix "${ROOT_DIR}/monitoring-ui" \
            -- --port "${vite_port}" --strictPort \
            > /tmp/durga-vite-${pid}.log 2>&1 &
    BG_PIDS+=($!)

    proc_idx=$((proc_idx + 1))
done

# ── 5. Wait for backends to be ready ────────────────────────────────────────
echo ""
banner "Waiting for backends"
for i in $(seq 1 30); do
    all_ready=true
    for ((j=0; j<proc_idx; j++)); do
        port=$((BACKEND_PORT + j))
        if ! curl -sf "http://localhost:${port}/health" > /dev/null 2>&1; then
            all_ready=false
            break
        fi
    done
    if [[ "${all_ready}" == "true" ]]; then
        break
    fi
    sleep 1
done

# ── 6. Summary ──────────────────────────────────────────────────────────────
echo ""
banner "Ready — open in browser"
echo ""
for ((j=0; j<proc_idx; j++)); do
    IFS=':' read -r pid bpmn interval <<< "${PROC_ARRAY[$j]}"
    vite_port=$((VITE_PORT + j))
    backend_port=$((BACKEND_PORT + j))
    printf "  ${BOLD}%s${RESET}\n" "${pid}"
    link "    SPA  → http://localhost:${vite_port}"
    info  "    API  → http://localhost:${backend_port}"
    echo ""
done
echo "Press Ctrl+C to stop all services."
echo ""

# ── 7. Keep alive ───────────────────────────────────────────────────────────
while true; do
    sleep 5
done
