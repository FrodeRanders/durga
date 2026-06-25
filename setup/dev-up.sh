#!/usr/bin/env bash
set -euo pipefail

# ── dev-up.sh ───────────────────────────────────────────────────────────────
# One-command full-stack demo: Kafka, monitoring backends (API + SPA),
# and process feed generators.  Each backend serves its own SPA directly —
# no separate frontend dev server needed.
#
# Usage:
#   ./setup/dev-up.sh
#
#   # Custom process:
#   PROCESS_ID=order_fulfillment BPMN_PATH=src/test/resources/bpmn/order_fulfillment.bpmn ./setup/dev-up.sh
#
#   # Multi-process overview (no diagram, all processes in one view):
#   PROCESS_ID=all ./setup/dev-up.sh
#
#   # Two specific processes side by side:
#   PROCESSES="invoice_receipt:src/test/resources/bpmn/invoice_receipt.bpmn,order_fulfillment:src/test/resources/bpmn/order_fulfillment.bpmn" \
#     ./setup/dev-up.sh
#
#   # Skip Docker Kafka (assume already running):
#   START_KAFKA=false ./setup/dev-up.sh
#
# Env vars:
#   PROCESS_ID     single process id (default invoice_receipt)
#   BPMN_PATH      path to BPMN file for this process
#   PROCESSES      comma-separated "pid:bpmnPath[:intervalMs]" for multi-backend
#   BOOTSTRAP      Kafka bootstrap servers (default localhost:9094)
#   BACKEND_PORT   backend API start port (default 8081)
#   START_KAFKA    auto-start Kafka via docker compose (default true)
#   SKIP_BUILD     skip Maven + npm build (default false)
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration defaults ──────────────────────────────────────────────────
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
BACKEND_PORT="${BACKEND_PORT:-8081}"
START_KAFKA="${START_KAFKA:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"
# Single-process defaults (used when PROCESSES is not set)
PROCESS_ID="${PROCESS_ID:-invoice_receipt}"
BPMN_PATH="${BPMN_PATH:-src/test/resources/bpmn/invoice_receipt.bpmn}"
FEED_INTERVAL="${FEED_INTERVAL:-1000}"
# Multi-process override
PROCESSES="${PROCESSES:-}"

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
    (cd monitoring-ui && npm install --silent 2>/dev/null && npm run build)
    info "Jar and SPA ready"
fi

# Resolve the JAR (handle version changes)
JAR="$(find "${ROOT_DIR}/target" -maxdepth 1 -name 'durga-*.jar' ! -name '*original*' -type f -print -quit)"
if [[ -z "${JAR}" || ! -f "${JAR}" ]]; then
    echo "ERROR: Could not find built Durga JAR under target/" >&2
    exit 1
fi

# ── 3. Build process list ───────────────────────────────────────────────────
declare -a PROC_PID=()
declare -a PROC_BPMN=()
declare -a PROC_INTERVAL=()

if [[ -n "${PROCESSES}" ]]; then
    IFS=',' read -ra PROC_ARRAY <<< "${PROCESSES}"
    for proc_def in "${PROC_ARRAY[@]}"; do
        IFS=':' read -r pid bpmn interval <<< "${proc_def}"
        PROC_PID+=("${pid}")
        PROC_BPMN+=("${bpmn}")
        PROC_INTERVAL+=("${interval:-1000}")
    done
else
    PROC_PID+=("${PROCESS_ID}")
    PROC_BPMN+=("${BPMN_PATH}")
    PROC_INTERVAL+=("${FEED_INTERVAL}")
fi
NUM_PROCS=${#PROC_PID[@]}

# ── 4. Clean up from previous runs ──────────────────────────────────────────
echo ""
banner "Cleaning up"
for ((j=0; j<NUM_PROCS; j++)); do
    pid="${PROC_PID[$j]}"
    port=$((BACKEND_PORT + j))
    # Kill anything holding target ports
    lsof -ti "tcp:${port}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    # Wipe stale Kafka Streams state
    rm -rf "/tmp/kafka-streams-state-${pid}" 2>/dev/null || true
done

# ── 5. Start backends and feeds ────────────────────────────────────────────
banner "Starting services"

for ((j=0; j<NUM_PROCS; j++)); do
    pid="${PROC_PID[$j]}"
    bpmn="${PROC_BPMN[$j]}"
    interval="${PROC_INTERVAL[$j]}"
    backend_port=$((BACKEND_PORT + j))
    app_id="durga-mon-${pid}"

    printf "\n  ${BOLD}%s${RESET}\n" "${pid}"
    info "API → http://localhost:${backend_port}"

    # ── Monitoring backend (API + SPA) ──────────────────────────────────────────
    if [[ "${pid}" == "all" ]]; then
      java -Ddurga.streams.state.dir=/tmp/kafka-streams-state-${pid} \
          -cp "${JAR}" \
          org.gautelis.durga.monitoring.MonitoringContainer \
          "${BOOTSTRAP}" \
          "${app_id}" \
          "${backend_port}" \
          "${pid}" \
          "" \
          "${ROOT_DIR}/monitoring-ui/dist" \
          "${ROOT_DIR}/src/test/resources/bpmn" \
          > "/tmp/durga-backend-${pid}.log" 2>&1 &
    elif [[ -n "${bpmn}" ]]; then
      java -Ddurga.streams.state.dir=/tmp/kafka-streams-state-${pid} \
          -cp "${JAR}" \
          org.gautelis.durga.monitoring.MonitoringContainer \
          "${BOOTSTRAP}" \
          "${app_id}" \
          "${backend_port}" \
          "${pid}" \
          "${bpmn}" \
          "${ROOT_DIR}/monitoring-ui/dist" \
          > "/tmp/durga-backend-${pid}.log" 2>&1 &
    else
      java -Ddurga.streams.state.dir=/tmp/kafka-streams-state-${pid} \
          -cp "${JAR}" \
          org.gautelis.durga.monitoring.MonitoringContainer \
          "${BOOTSTRAP}" \
          "${app_id}" \
          "${backend_port}" \
          "${pid}" \
          "" \
          "${ROOT_DIR}/monitoring-ui/dist" \
          > "/tmp/durga-backend-${pid}.log" 2>&1 &
    fi
    BG_PIDS+=($!)

    # ── Continuous feed (skip for multi-process mode) ────────────────────────
    if [[ "${pid}" != "all" ]]; then
      java -cp "${JAR}" \
          org.gautelis.durga.demo.ContinuousFeedPublisher \
          "${BOOTSTRAP}" \
          "${pid}" \
          "${interval}" \
          > /tmp/durga-feed-${pid}.log 2>&1 &
      BG_PIDS+=($!)
    fi
done

# ── 6. Wait for backends ────────────────────────────────────────────────────
echo ""
banner "Waiting for backends"
for i in $(seq 1 30); do
    all_ready=true
    for ((j=0; j<NUM_PROCS; j++)); do
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

# ── 7. Summary ──────────────────────────────────────────────────────────────
echo ""
banner "Ready — open in browser"
echo ""
for ((j=0; j<NUM_PROCS; j++)); do
    pid="${PROC_PID[$j]}"
    backend_port=$((BACKEND_PORT + j))
    printf "  ${BOLD}%s${RESET}\n" "${pid}"
    link "    SPA + API  → http://localhost:${backend_port}"
    echo ""
done

if [[ ${NUM_PROCS} -gt 1 ]]; then
    echo "Each backend serves its own SPA. Open both URLs in separate tabs."
    echo ""
fi
echo "Press Ctrl+C to stop all services."
echo ""

# ── 8. Keep alive ───────────────────────────────────────────────────────────
while true; do
    sleep 5
done
