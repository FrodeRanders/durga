#!/usr/bin/env bash
set -euo pipefail

# ── dev-up.sh ───────────────────────────────────────────────────────────────
# One-command full-stack demo: Kafka, monitoring backend (API + SPA),
# and process feed generators.  Processes self-register their BPMN models
# via the process-models Kafka topic — no pre-configured process list needed.
#
# Usage:
#   ./setup/dev-up.sh
#
#   # Customise or add feeds:
#   FEED_PIDS="invoice_receipt,order_fulfillment" ./setup/dev-up.sh
#
#   # Skip Docker Kafka (assume already running):
#   START_KAFKA=false ./setup/dev-up.sh
#
# Env vars:
#   FEED_PIDS      comma-separated process IDs to auto-feed (default invoice_receipt,order_fulfillment)
#   BOOTSTRAP      Kafka bootstrap servers (default localhost:9094)
#   PORT           backend API port (default 8081)
#   START_KAFKA    auto-start Kafka via docker compose (default true)
#   SKIP_BUILD     skip Maven + npm build (default false)
#   BPMN_DIR       directory of {pid}.bpmn files for diagram fallback
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration ──────────────────────────────────────────────────────────
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
PORT="${PORT:-8081}"
START_KAFKA="${START_KAFKA:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"
FEED_PIDS="${FEED_PIDS:-invoice_receipt,order_fulfillment}"
FEED_INTERVAL="${FEED_INTERVAL:-1000}"
BPMN_DIR="${BPMN_DIR:-${ROOT_DIR}/durga-tools/src/test/resources/bpmn}"

declare -a BG_PIDS=()

_cleanup() {
    echo ""
    echo "━━━ Shutting down ━━━"
    for p in "${BG_PIDS[@]}"; do
        kill "${p}" 2>/dev/null || true
    done
    sleep 2
    for p in "${BG_PIDS[@]}"; do
        kill -9 "${p}" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap _cleanup EXIT INT TERM

BOLD="\033[1m"; GREEN="\033[32m"; CYAN="\033[36m"; RESET="\033[0m"
banner()  { printf "${BOLD}━━━ %s ━━━${RESET}\n" "$1"; }
info()    { printf "  ${GREEN}→${RESET} %s\n" "$1"; }
link()    { printf "  ${CYAN}%s${RESET}\n" "$1"; }

# ── Prerequisites ──────────────────────────────────────────────────────────
for cmd in java mvn node npm; do
    if ! command -v "${cmd}" &> /dev/null; then
        echo "ERROR: '${cmd}' is required but not found in PATH" >&2; exit 1
    fi
done

# ── 1. Kafka ───────────────────────────────────────────────────────────────
if [[ "${START_KAFKA}" == "true" ]]; then
    banner "Starting Kafka"
    (cd "${SCRIPT_DIR}" && docker compose up -d)
    info "Waiting for Kafka..."
    for i in $(seq 1 30); do
        if docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka_b \
            kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
            break
        fi; sleep 1
    done
    info "Kafka broker: ${BOOTSTRAP}"
    info "Kafka UI:     http://localhost:8080"
fi

# ── 2. Build ───────────────────────────────────────────────────────────────
if [[ "${SKIP_BUILD}" != "true" ]]; then
    banner "Building"
    cd "${ROOT_DIR}"
    mkdir -p monitoring-ui/dist && touch monitoring-ui/dist/.gitkeep
    (cd monitoring-ui && npm install --silent 2>/dev/null && npm run build)
    mvn -q package -DskipTests
    info "Jar and SPA ready"
fi

JAR="$(find "${ROOT_DIR}/durga-monitor/target" -maxdepth 1 -name 'durga-monitor-*-runner.jar' -type f -print -quit)"
if [[ -z "${JAR}" || ! -f "${JAR}" ]]; then
    echo "ERROR: Could not find built Durga monitor runner JAR under durga-monitor/target/" >&2; exit 1
fi

DEMO_JAR="$(find "${ROOT_DIR}/durga-demo/target" -maxdepth 1 -name 'durga-demo-*.jar' ! -name 'original-*' -type f -print -quit)"
if [[ -z "${DEMO_JAR}" || ! -f "${DEMO_JAR}" ]]; then
    echo "ERROR: Could not find built Durga demo JAR under durga-demo/target/" >&2; exit 1
fi

# ── 3. Clean up ────────────────────────────────────────────────────────────
echo ""
banner "Cleaning up"
lsof -ti "tcp:${PORT}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
rm -rf "/tmp/kafka-streams-state-all" 2>/dev/null || true

# ── 4. Start monitoring backend ────────────────────────────────────────────
banner "Starting monitor (all processes)"
java -Dquarkus.http.port="${PORT}" \
    -Ddurga.streams.state.dir=/tmp/kafka-streams-state-all \
    -jar "${JAR}" \
    "${BOOTSTRAP}" \
    "durga-monitor" \
    > /tmp/durga-backend.log 2>&1 &
BG_PIDS+=($!)

info "Monitor → http://localhost:${PORT}"

# ── 5. Publish all BPMN models from bpmn directory ─────────────────────────
if [[ -d "${BPMN_DIR}" ]]; then
    banner "Registering BPMN models"
    for bpmn in "${BPMN_DIR}"/*.bpmn; do
        fname="$(basename "${bpmn}" .bpmn)"
        java -cp "${DEMO_JAR}" \
            org.gautelis.durga.demo.BpmnModelPublisher \
            "${BOOTSTRAP}" \
            "${fname}" \
            "${bpmn}" \
            > /dev/null 2>&1 &
        BG_PIDS+=($!)
        info "  ${fname}"
    done
fi

# ── 6. Start feed generators ───────────────────────────────────────────────
banner "Starting feed generators"
IFS=',' read -ra PIDS <<< "${FEED_PIDS}"
for pid in "${PIDS[@]}"; do
    java -cp "${DEMO_JAR}" \
        org.gautelis.durga.demo.ContinuousFeedPublisher \
        "${BOOTSTRAP}" \
        "${pid}" \
        "${FEED_INTERVAL}" \
        "${BPMN_DIR}" \
        > "/tmp/durga-feed-${pid}.log" 2>&1 &
    BG_PIDS+=($!)
    info "  ${pid} (${FEED_INTERVAL}ms)"
done

# ── 7. Wait ────────────────────────────────────────────────────────────────
echo ""
banner "Waiting for backend"
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${PORT}/health" > /dev/null 2>&1; then
        break
    fi; sleep 1
done

# ── 8. Summary ─────────────────────────────────────────────────────────────
echo ""
banner "Ready — open in browser"
echo ""
link "    SPA + API  → http://localhost:${PORT}"
echo ""

echo "Feeding ${#PIDS[@]} process(es). Click a process in the inventory to drill down."
echo ""
echo "Press Ctrl+C to stop all services."
echo ""

# ── 9. Keep alive ──────────────────────────────────────────────────────────
while true; do sleep 5; done
