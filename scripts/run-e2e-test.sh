#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/setup/e2e/docker-compose.yml"

REDPANDA_BOOTSTRAP="localhost:9094"
TEST_DATA_LOADER="org.gautelis.durga.demo.E2ETestDataLoader"
INFRA_LOCK="$PROJECT_DIR/.e2e-infra-running"
MONITOR_PORT=8081
MONITOR_JAR="$PROJECT_DIR/target/durga-0.1.0-beta.1-runner.jar"
MONITOR_LOG="$PROJECT_DIR/target/monitor.log"
MONITOR_PID_FILE="$PROJECT_DIR/target/monitor.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[e2e]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[err]${NC} $*" >&2; }

usage() {
    cat <<EOF
Usage: run-e2e-test.sh <command> [options]

Commands:
  test       Run E2E integration tests (self-contained, uses Testcontainers)
  feed       Start data feed + monitoring server (auto-starts infra)
  test-run   Run tests, then start feed + monitoring server (Ctrl-C to stop)
  stop       Stop infrastructure and monitoring server

Feed options (with 'feed' or 'test-run'):
  --count N       Publish exactly N records then exit (default: continuous)
  --interval-ms   Batch interval in ms (default: 100)

Examples:
  ./run-e2e-test.sh test              # Run all integration tests
  ./run-e2e-test.sh feed              # Infra + monitor + continuous feed
  ./run-e2e-test.sh feed --count 200  # Infra + monitor + 200 records
  ./run-e2e-test.sh test-run          # Test + monitor + continuous feed
  ./run-e2e-test.sh stop              # Tear down everything
EOF
}

# ------- internal: infra -------

infra_running() {
    docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q durga-redpanda
}

start_infra() {
    if infra_running; then
        log "Infrastructure already running."
        return
    fi
    log "Starting infrastructure (Redpanda, PostgreSQL, Kafka Connect)..."
    docker compose -f "$COMPOSE_FILE" up -d

    log "Waiting for Redpanda..."
    until docker exec durga-redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; do sleep 1; done
    log "Redpanda ready ($REDPANDA_BOOTSTRAP)"

    log "Waiting for PostgreSQL..."
    until docker exec durga-postgres pg_isready -U durga 2>/dev/null | grep -q 'accepting'; do sleep 1; done
    log "PostgreSQL ready"

    log "Waiting for Kafka Connect..."
    until curl -s http://localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done
    log "Kafka Connect ready"

    docker exec durga-redpanda rpk topic create \
        process-events-e2e-pipeline vannak-metadata-events process-models 2>/dev/null || true

    touch "$INFRA_LOCK"
    log "Infrastructure ready."
}

stop_infra() {
    if infra_running || [ -f "$INFRA_LOCK" ]; then
        log "Stopping infrastructure..."
        docker compose -f "$COMPOSE_FILE" down -v
        rm -f "$INFRA_LOCK"
        log "Stopped."
    fi
}

# ------- internal: monitoring server -------

build_monitor() {
    if [ -f "$MONITOR_JAR" ] && [ -z "$(find "$PROJECT_DIR/src/main" "$PROJECT_DIR/monitoring-ui" "$PROJECT_DIR/pom.xml" -newer "$MONITOR_JAR" -print -quit 2>/dev/null)" ]; then
        return
    fi
    log "Building monitoring server (Quarkus uber-JAR)..."
    cd "$PROJECT_DIR"
    mvn -q package -Pmonitoring -DskipTests
    if [ ! -f "$MONITOR_JAR" ]; then
        err "Monitoring JAR not found at $MONITOR_JAR. Build may have failed."
        exit 1
    fi
    log "Monitoring server built."
}

start_monitor() {
    if [ -f "$MONITOR_PID_FILE" ] && kill -0 "$(cat "$MONITOR_PID_FILE")" 2>/dev/null; then
        log "Monitoring server already running (pid $(cat "$MONITOR_PID_FILE"))."
        return
    fi

    build_monitor

    log "Starting monitoring server on port $MONITOR_PORT..."
    java -Dkafka.bootstrap.servers="$REDPANDA_BOOTSTRAP" \
         -Dquarkus.http.port="$MONITOR_PORT" \
         -jar "$MONITOR_JAR" \
         > "$MONITOR_LOG" 2>&1 &
    echo $! > "$MONITOR_PID_FILE"

    log "Waiting for monitoring server..."
    local deadline=$(($(date +%s) + 60))
    while [ $(date +%s) -lt $deadline ]; do
        if curl -s "http://localhost:$MONITOR_PORT/health" | grep -q RUNNING; then
            log "Monitoring server ready: http://localhost:$MONITOR_PORT"
            log "  Dashboard: http://localhost:$MONITOR_PORT/"
            log "  Health:    curl http://localhost:$MONITOR_PORT/health"
            log "  Metrics:   curl http://localhost:$MONITOR_PORT/api/metrics"
            return
        fi
        sleep 1
    done
    err "Monitoring server failed to start within 60s. Check $MONITOR_LOG"
    stop_monitor
    exit 1
}

stop_monitor() {
    if [ -f "$MONITOR_PID_FILE" ]; then
        local pid=$(cat "$MONITOR_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping monitoring server (pid $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$MONITOR_PID_FILE"
    fi
}

# ------- commands -------

cmd_test() {
    log "Running E2E integration tests (Testcontainers, requires Docker)..."
    cd "$PROJECT_DIR"
    mvn -q compile
    mvn test -Dtest='*IntegrationTest' -Ddurga.it.timeout.seconds=120 -DfailIfNoTests=false
    log "Tests complete."
}

cmd_feed() {
    local count="-1"
    local interval="100"

    while [ $# -gt 0 ]; do
        case "$1" in
            --count) count="$2"; shift 2;;
            --interval-ms) interval="$2"; shift 2;;
            *) shift;;
        esac
    done

    trap 'stop_monitor; stop_infra' EXIT INT TERM
    start_infra
    start_monitor
    echo ""

    cd "$PROJECT_DIR"

    local mode="continuous (Ctrl-C to stop)"
    [ "$count" != "-1" ] && mode="bounded: $count records"

    log "Starting data feed — $mode"
    echo ""
    local feed_args=(
        --bootstrap-servers "$REDPANDA_BOOTSTRAP"
        --topic e2e_pipeline_start
        --interval-ms "$interval"
    )
    if [ "$count" != "-1" ]; then
        feed_args+=(--count "$count")
    fi
    java -cp "$MONITOR_JAR" "$TEST_DATA_LOADER" "${feed_args[@]}"
}

cmd_test_run() {
    local feed_count="-1"
    local feed_interval="100"

    while [ $# -gt 0 ]; do
        case "$1" in
            --count) feed_count="$2"; shift 2;;
            --interval-ms) feed_interval="$2"; shift 2;;
            *) shift;;
        esac
    done

    trap 'stop_monitor; stop_infra' EXIT INT TERM
    start_infra

    cmd_test
    echo ""
    warn "Tests passed. Starting monitoring server and data feed."
    warn "Press Ctrl-C to stop and clean up."
    echo ""

    start_monitor
    echo ""
    cd "$PROJECT_DIR"

    local mode="continuous (Ctrl-C to stop)"
    [ "$feed_count" != "-1" ] && mode="bounded: $feed_count records"

    log "Starting data feed — $mode"
    echo ""
    local feed_args=(
        --bootstrap-servers "$REDPANDA_BOOTSTRAP"
        --topic e2e_pipeline_start
        --interval-ms "$feed_interval"
    )
    if [ "$feed_count" != "-1" ]; then
        feed_args+=(--count "$feed_count")
    fi
    java -cp "$MONITOR_JAR" "$TEST_DATA_LOADER" "${feed_args[@]}"
}

cmd_stop() {
    stop_monitor
    stop_infra
}

# ------- main -------

if [ $# -eq 0 ]; then
    usage
    exit 1
fi

CMD="$1"
shift

case "$CMD" in
    test)     cmd_test "$@";;
    feed)     cmd_feed "$@";;
    test-run) cmd_test_run "$@";;
    stop)     cmd_stop "$@";;
    *)        usage; exit 1;;
esac
