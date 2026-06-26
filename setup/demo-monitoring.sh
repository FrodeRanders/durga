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

mvn -q package -Pmonitoring

JAR="$(find target -maxdepth 1 -name 'durga-*-runner.jar' -type f -exec ls -t {} + 2>/dev/null | head -n 1 || true)"
if [[ -z "${JAR}" || ! -f "${JAR}" ]]; then
  echo "Could not find built Durga JAR under target/" >&2
  exit 1
fi

java -Dquarkus.http.port="${HTTP_PORT}" \
  -jar "${JAR}" \
  "${BOOTSTRAP}" \
  "${APPLICATION_ID}" &
MONITOR_PID=$!

cleanup() {
  if kill -0 "${MONITOR_PID}" >/dev/null 2>&1; then
    kill "${MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${MONITOR_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  health_output=$(java -cp "${JAR}" \
    org.gautelis.durga.monitoring.ProcessMonitoringClient \
    "http://localhost:${HTTP_PORT}" \
    health 2>/dev/null || true)
  if echo "${health_output}" | grep -q '"RUNNING"'; then
    break
  fi
  sleep 1
done

publish_output=$(java -cp "${JAR}" \
  org.gautelis.durga.demo.ProcessEventScenarioRunner \
  "${BOOTSTRAP}" \
  "${SCENARIO}" \
  "${PROCESS_ID}" \
  "${ACTIVITIES}")

echo "${publish_output}"
instance_id=$(echo "${publish_output}" | grep -oE 'instanceId=([^[:space:]]+)' | cut -d= -f2)

sleep 2

echo
echo "Health"
java -cp "${JAR}" \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  health

echo
echo "Counts"
java -cp "${JAR}" \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  counts \
  "${PROCESS_ID}"

echo
echo "Latency"
java -cp "${JAR}" \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  latency \
  "${PROCESS_ID}"

echo
echo "Stuck"
java -cp "${JAR}" \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  stuck \
  "${PROCESS_ID}" \
  1

echo
echo "Instance"
java -cp "${JAR}" \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  "http://localhost:${HTTP_PORT}" \
  instance \
  "${instance_id}"
