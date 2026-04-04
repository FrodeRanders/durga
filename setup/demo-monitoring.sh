#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-localhost:9094}
HTTP_PORT=${HTTP_PORT:-18081}
APPLICATION_ID=${APPLICATION_ID:-durga-monitoring-demo}
PROCESS_ID=${PROCESS_ID:-invoice_receipt}
ACTIVITIES=${ACTIVITIES:-register_invoice,review_invoice,notify_requester}
SCENARIO=${SCENARIO:-happy}
START_KAFKA=${START_KAFKA:-false}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${ROOT_DIR}/.." && pwd)"

if [[ "${START_KAFKA}" == "true" ]]; then
  (cd "${ROOT_DIR}" && docker compose up -d)
fi

cd "${PROJECT_ROOT}"

mvn -q package

java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringApp \
  "${BOOTSTRAP}" \
  "${APPLICATION_ID}" \
  "${HTTP_PORT}" &
MONITOR_PID=$!

cleanup() {
  if kill -0 "${MONITOR_PID}" >/dev/null 2>&1; then
    kill "${MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${MONITOR_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if java -cp target/durga-1.0-SNAPSHOT.jar \
    org.gautelis.durga.monitoring.ProcessMonitoringClient \
    "http://localhost:${HTTP_PORT}" \
    health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

publish_output=$(java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessEventScenarioRunner \
  "${BOOTSTRAP}" \
  "${SCENARIO}" \
  "${PROCESS_ID}" \
  "${ACTIVITIES}")

echo "${publish_output}"
instance_id="${publish_output##*=}"

sleep 2

echo
echo "Health"
java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  health

echo
echo "Counts"
java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  counts \
  "${PROCESS_ID}"

echo
echo "Latency"
java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  latency \
  "${PROCESS_ID}"

echo
echo "Stuck"
java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  stuck \
  "${PROCESS_ID}" \
  1

echo
echo "Instance"
java -cp target/durga-1.0-SNAPSHOT.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  instance \
  "${instance_id}"
