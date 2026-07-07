#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/setup/e2e/docker-compose.yml"

REDPANDA_BOOTSTRAP="localhost:9094"
SCAFFOLDER="org.gautelis.durga.tools.BpmnScaffolder"
FEED_PUBLISHER="org.gautelis.durga.demo.E2EFeedPublisher"
E2E_BPMN_PATH="$PROJECT_DIR/durga-tools/src/test/resources/bpmn/e2e_pipeline.bpmn"
E2E_PROCESS_ID=$(grep -o 'bpmn:process[^>]*id="[^"]*"' "$E2E_BPMN_PATH" | grep -o 'id="[^"]*"' | head -1 | cut -d'"' -f2)
if [ -z "$E2E_PROCESS_ID" ]; then
    E2E_PROCESS_ID="e2e_pipeline"
    warn "Could not extract process ID from $E2E_BPMN_PATH, defaulting to $E2E_PROCESS_ID"
fi
INFRA_LOCK="$PROJECT_DIR/.e2e-infra-running"
MONITOR_PORT=8081
MONITOR_JAR="$PROJECT_DIR/durga-monitor/target/durga-monitor-0.1.0-beta.1-runner.jar"
TOOLS_JAR="$PROJECT_DIR/durga-tools/target/durga-tools-0.1.0-beta.1.jar"
MONITOR_LOG="$PROJECT_DIR/target/monitor.log"
MONITOR_PID_FILE="$PROJECT_DIR/target/monitor.pid"
GEN_DIR="$PROJECT_DIR/target/e2e-gen"
GEN_PID_FILE="$PROJECT_DIR/target/pipeline.pid"
GEN_LOG="$PROJECT_DIR/target/pipeline.log"
MONITOR_STATE_DIR="$PROJECT_DIR/target/e2e-kafka-streams-state"
FAULT_STATE_DIR="$PROJECT_DIR/target/e2e-kafka-streams-fault-state"
ALARM_STATE_DIR="$PROJECT_DIR/target/e2e-kafka-streams-alarm-state"
ALARM_WATCH_PID_FILE="$PROJECT_DIR/target/alarm-watch.pid"
RUST_WORKERS_PID_FILE="$PROJECT_DIR/target/rust-workers.pids"
DURGA_RUST_DIR="$PROJECT_DIR/durga-rust"
DURGA_RUST_BUILD_DIR="$PROJECT_DIR/durga-rust-build"

# Code-generation target for the pipeline: 'java' (Quarkus) or 'rust' (Cargo
# workers). The monitoring server is Java in both cases and observes either via
# the shared process-events topic. Set with --target on feed/test-run.
TARGET="java"

# Automatic (monitor-owned) alarm tuning. Defaults are deliberately sensitive so a
# stall cascade becomes detectable within a short e2e run; override via env if needed.
STUCK_TIMEOUT_SECONDS="${STUCK_TIMEOUT_SECONDS:-20}"
STUCK_SEVERITY="${STUCK_SEVERITY:-WARN}"
CASCADE_WINDOW_SECONDS="${CASCADE_WINDOW_SECONDS:-30}"
CASCADE_THRESHOLD="${CASCADE_THRESHOLD:-3}"
CASCADE_SEVERITY="${CASCADE_SEVERITY:-CRITICAL}"
ALARM_SCAN_INTERVAL_MS="${ALARM_SCAN_INTERVAL_MS:-5000}"
ALARM_WATCH_INTERVAL_SECONDS="${ALARM_WATCH_INTERVAL_SECONDS:-5}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[e2e]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[err]${NC} $*" >&2; }

validate_target() {
    if [ "$TARGET" != "java" ] && [ "$TARGET" != "rust" ]; then
        err "--target must be 'java' or 'rust' (got '$TARGET')"
        exit 1
    fi
}

usage() {
    cat <<EOF
Usage: run-e2e-test.sh <command> [options]

Commands:
  test       Run E2E integration tests (self-contained, uses Testcontainers)
  feed       Full e2e: infra + monitor + scaffold + build + pipeline + data feed
  test-run   Run tests, then full e2e pipeline (Ctrl-C to stop)
  stop       Stop infrastructure, pipeline, and monitoring server

Feed options (with 'feed' or 'test-run'):
  --target T      Pipeline code-gen target: 'java' (default) or 'rust'
  --count N       Publish exactly N records then exit (default: continuous)
  --interval-ms   Interval in ms between records (default: 100)

The 'rust' target scaffolds the process with --target rust, builds the Cargo
project (one worker binary per BPMN task/gateway; requires a Rust toolchain and
cmake for librdkafka), and runs each worker as a separate process. The Java
monitor observes the Rust workers unchanged via process-events-<processId>.

Cascade detection (automatic, monitor-owned — no BPMN alarm config needed):
  The 'feed' and 'test-run' commands start a background watcher that polls
  the monitor and reports STUCK instance alarms and, when a surge stalls at
  once, the CASCADE alarm. Tune via environment variables:
    STUCK_TIMEOUT_SECONDS   idle time before an instance is "stuck" (default 20)
    CASCADE_WINDOW_SECONDS  rolling window for the surge (default 30)
    CASCADE_THRESHOLD       stuck onsets in window before cascade fires (default 3)
    ALARM_SCAN_INTERVAL_MS  monitor stall scan cadence (default 5000)

Examples:
  ./run-e2e-test.sh test              # Run all integration tests
  ./run-e2e-test.sh feed              # Full e2e pipeline + monitoring (continuous)
  ./run-e2e-test.sh feed --count 200  # Bounded: 200 orders through the pipeline
  ./run-e2e-test.sh feed --target rust # Same pipeline, generated as Rust workers
  ./run-e2e-test.sh test-run          # Test + full e2e pipeline
  ./run-e2e-test.sh stop              # Tear down
  CASCADE_THRESHOLD=2 ./run-e2e-test.sh feed   # More sensitive cascade
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

    log "Creating topics..."
    docker exec durga-redpanda rpk topic create \
        e2e_pipeline_start process-events-e2e_pipeline vannak-metadata-events process-models 2>/dev/null || true

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

# ------- internal: build -------

build_all() {
    if [ -f "$TOOLS_JAR" ] && [ -f "$MONITOR_JAR" ] \
       && [ -z "$(find "$PROJECT_DIR/durga-runtime/src" "$PROJECT_DIR/durga-tools/src" "$PROJECT_DIR/durga-monitor/src" "$PROJECT_DIR/monitoring-ui" -newer "$TOOLS_JAR" -print -quit 2>/dev/null)" ]; then
        return
    fi
    log "Building durga modules..."
    cd "$PROJECT_DIR"
    mvn -q install -DskipTests -pl durga-runtime,durga-tools
    mvn -q package -DskipTests -pl durga-monitor
    if [ ! -f "$TOOLS_JAR" ] || [ ! -f "$MONITOR_JAR" ]; then
        err "Build failed. Check output above."
        exit 1
    fi
    log "Build complete."
}

# ------- internal: monitoring server -------

start_monitor() {
    if [ -f "$MONITOR_PID_FILE" ] && kill -0 "$(cat "$MONITOR_PID_FILE")" 2>/dev/null; then
        log "Monitoring server already running (pid $(cat "$MONITOR_PID_FILE"))."
        return
    fi

    rm -rf "$MONITOR_STATE_DIR" "$FAULT_STATE_DIR" "$ALARM_STATE_DIR"

    log "Starting monitoring server on port $MONITOR_PORT..."
    java -Dkafka.bootstrap.servers="$REDPANDA_BOOTSTRAP" \
         -Dquarkus.http.port="$MONITOR_PORT" \
         -Ddurga.streams.state.dir="$MONITOR_STATE_DIR" \
         -Ddurga.fault.streams.state.dir="$FAULT_STATE_DIR" \
         -Ddurga.alarm.streams.state.dir="$ALARM_STATE_DIR" \
         -Ddurga.alarm.stuck.timeoutSeconds="$STUCK_TIMEOUT_SECONDS" \
         -Ddurga.alarm.stuck.severity="$STUCK_SEVERITY" \
         -Ddurga.alarm.cascade.windowSeconds="$CASCADE_WINDOW_SECONDS" \
         -Ddurga.alarm.cascade.threshold="$CASCADE_THRESHOLD" \
         -Ddurga.alarm.cascade.severity="$CASCADE_SEVERITY" \
         -Ddurga.alarm.scan.interval.ms="$ALARM_SCAN_INTERVAL_MS" \
         -jar "$MONITOR_JAR" \
         > "$MONITOR_LOG" 2>&1 &
    echo $! > "$MONITOR_PID_FILE"

    log "Waiting for monitoring server..."
    local deadline=$(($(date +%s) + 60))
    while [ $(date +%s) -lt $deadline ]; do
        if curl -s "http://localhost:$MONITOR_PORT/api/health" | grep -q RUNNING; then
            log "Monitoring server ready: http://localhost:$MONITOR_PORT"
            return
        fi
        sleep 1
    done
    err "Monitoring server failed to start within 60s."
    stop_monitor
    exit 1
}

stop_monitor() {
    if [ -f "$MONITOR_PID_FILE" ]; then
        local pid=$(cat "$MONITOR_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping monitoring server (pid $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$MONITOR_PID_FILE"
    fi
}

# ------- internal: cascade detection -------

# Counts occurrences of a syndrome in the /api/alarms JSON payload.
count_syndrome() {
    local body="$1" syndrome="$2"
    printf '%s' "$body" | grep -o "\"syndrome\":\"$syndrome\"" | wc -l | tr -d ' '
}

# Background watcher that polls the monitor's alarm read-model and reports the
# automatic (monitor-owned) alarms as they appear — in particular the CASCADE
# alarm that fires when a surge of instances stall, even though e2e_pipeline.bpmn
# configures no alarms of its own.
watch_alarms() {
    local prev_stuck=0 prev_cascade=0
    while true; do
        local body stuck cascade
        body=$(curl -s "http://localhost:$MONITOR_PORT/api/alarms" 2>/dev/null || true)
        if [ -n "$body" ]; then
            stuck=$(count_syndrome "$body" STUCK)
            cascade=$(count_syndrome "$body" CASCADE)

            if [ "${stuck:-0}" -gt "$prev_stuck" ]; then
                warn "Stall detector: $stuck stuck-instance alarm scope(s) active (idle > ${STUCK_TIMEOUT_SECONDS}s)"
                prev_stuck=$stuck
            fi

            if [ "${cascade:-0}" -gt 0 ] && [ "$prev_cascade" -eq 0 ]; then
                err "=============================================================="
                err " CASCADE ALARM RAISED by the monitor (automatic layer)"
                err " >${CASCADE_THRESHOLD} instances stalled within ${CASCADE_WINDOW_SECONDS}s"
                err " No alarm was configured on e2e_pipeline.bpmn — this is the"
                err " monitor's own always-on detection."
                err "--------------------------------------------------------------"
                printf '%s' "$body" \
                    | tr ',' '\n' \
                    | grep -A0 -E '"(syndrome|severity|lastMessage|fireCount)"' \
                    | sed 's/^/   /' >&2 || true
                err "=============================================================="
                prev_cascade=$cascade
            fi
        fi
        sleep "$ALARM_WATCH_INTERVAL_SECONDS"
    done
}

start_alarm_watch() {
    if [ -f "$ALARM_WATCH_PID_FILE" ] && kill -0 "$(cat "$ALARM_WATCH_PID_FILE")" 2>/dev/null; then
        return
    fi
    log "Starting cascade detector (stuck timeout=${STUCK_TIMEOUT_SECONDS}s, cascade threshold>${CASCADE_THRESHOLD} in ${CASCADE_WINDOW_SECONDS}s)..."
    watch_alarms &
    echo $! > "$ALARM_WATCH_PID_FILE"
}

stop_alarm_watch() {
    if [ -f "$ALARM_WATCH_PID_FILE" ]; then
        local pid=$(cat "$ALARM_WATCH_PID_FILE")
        kill "$pid" 2>/dev/null || true
        rm -f "$ALARM_WATCH_PID_FILE"
    fi
}

# ------- internal: generated pipeline -------

scaffold_and_build_pipeline() {
    if [ "$TARGET" = "rust" ]; then
        scaffold_and_build_pipeline_rust
        return
    fi
    log "Scaffolding $E2E_PROCESS_ID..."
    rm -rf "$GEN_DIR"
    java -cp "$TOOLS_JAR" "$SCAFFOLDER" "$E2E_BPMN_PATH" "$GEN_DIR"
    log "Scaffolding complete."

    # Remove generated runtime classes that are provided by durga-runtime.
    # The scaffolder generates these for standalone use; when durga-runtime is
    # a dependency, they cause duplicate-class / version-skew errors.
    # We keep ModelEnricher (in tools/) because it's needed at build time
    # and ModelRegistration (runtime bean, process-specific).
    local core_dir="$GEN_DIR/src/main/java/org/gautelis/durga"
    if [ -d "$core_dir" ]; then
        for f in ProcessEvent ProcessState ProcessStateStore ScopeCancellationRegistry VannakMetadata DataHandle DataIndividualMetadataEvent; do
            rm -f "$core_dir/${f}.java"
        done
        rm -rf "$core_dir/plugins"
    fi

    log "Building generated pipeline..."
    cd "$GEN_DIR"
    mvn -q package -DskipTests
    local jar=$(echo "$GEN_DIR"/target/*-runner.jar)
    if [ ! -f "$jar" ]; then
        err "Pipeline runner JAR not found."
        exit 1
    fi
    log "Pipeline built: $jar"
}

# ------- internal: generated pipeline (rust target) -------

scaffold_and_build_pipeline_rust() {
    if ! command -v cargo >/dev/null 2>&1; then
        err "cargo not found — install a Rust toolchain to use --target rust."
        exit 1
    fi
    log "Scaffolding $E2E_PROCESS_ID (Rust target)..."
    rm -rf "$GEN_DIR"
    java -Ddurga.rust.crate.path="$DURGA_RUST_DIR" -Ddurga.rust.build.crate.path="$DURGA_RUST_BUILD_DIR" -cp "$TOOLS_JAR" "$SCAFFOLDER" \
        "$E2E_BPMN_PATH" --out "$GEN_DIR" --process-id "$E2E_PROCESS_ID" --target rust
    log "Scaffolding complete."

    log "Building generated Rust workers (cargo build --release)..."
    ( cd "$GEN_DIR" && cargo build --release ) || { err "cargo build failed."; exit 1; }
    log "Rust workers built."
}

create_rust_topics() {
    local topics=()
    local f name
    # Rust uses the same chaining-topic naming as java: each task/gateway has an output stream
    # (<name>_output), a gateway-fed inbox (<name>_input), and a DLQ (<name>_dlq).
    for f in "$GEN_DIR"/src/bin/*.rs; do
        name=$(basename "$f" .rs)
        topics+=("${E2E_PROCESS_ID}_${name}_output" \
                 "${E2E_PROCESS_ID}_${name}_input" \
                 "${E2E_PROCESS_ID}_${name}_dlq")
    done
    if [ ${#topics[@]} -gt 0 ]; then
        log "Creating Rust worker topics..."
        docker exec durga-redpanda rpk topic create "${topics[@]}" 2>/dev/null || true
    fi
}

start_pipeline_rust() {
    if [ -f "$RUST_WORKERS_PID_FILE" ] && kill -0 "$(head -1 "$RUST_WORKERS_PID_FILE" 2>/dev/null)" 2>/dev/null; then
        log "Rust workers already running."
        return
    fi
    if [ ! -d "$GEN_DIR/target/release" ]; then
        scaffold_and_build_pipeline_rust
    fi
    create_rust_topics

    : > "$RUST_WORKERS_PID_FILE"
    : > "$GEN_LOG"
    log "Starting generated Rust workers..."
    local f name bin count=0
    for f in "$GEN_DIR"/src/bin/*.rs; do
        name=$(basename "$f" .rs)
        bin="$GEN_DIR/target/release/$name"
        if [ ! -x "$bin" ]; then
            warn "  missing binary: $bin"
            continue
        fi
        KAFKA_BROKERS="$REDPANDA_BOOTSTRAP" "$bin" >> "$GEN_LOG" 2>&1 &
        echo $! >> "$RUST_WORKERS_PID_FILE"
        log "  started worker: $name (pid $!)"
        count=$((count + 1))
    done
    sleep 2
    log "Rust workers ready ($count processes; logs -> $GEN_LOG)."
}

stop_rust_workers() {
    if [ ! -f "$RUST_WORKERS_PID_FILE" ]; then
        return
    fi
    log "Stopping Rust workers..."
    local pid
    while read -r pid; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done < "$RUST_WORKERS_PID_FILE"
    sleep 1
    while read -r pid; do
        [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
    done < "$RUST_WORKERS_PID_FILE"
    rm -f "$RUST_WORKERS_PID_FILE"
}

start_pipeline() {
    if [ "$TARGET" = "rust" ]; then
        start_pipeline_rust
        return
    fi
    if [ -f "$GEN_PID_FILE" ] && kill -0 "$(cat "$GEN_PID_FILE")" 2>/dev/null; then
        log "Pipeline already running (pid $(cat "$GEN_PID_FILE"))."
        return
    fi

    local jar=$(echo "$GEN_DIR"/target/*-runner.jar)
    if [ ! -f "$jar" ]; then
        scaffold_and_build_pipeline
        jar=$(echo "$GEN_DIR"/target/*-runner.jar)
    fi

    log "Starting generated pipeline..."
    java -Dkafka.bootstrap.servers="$REDPANDA_BOOTSTRAP" \
         -Dquarkus.http.port=0 \
         -jar "$jar" \
         > "$GEN_LOG" 2>&1 &
    echo $! > "$GEN_PID_FILE"

    log "Waiting for pipeline to boot..."
    local deadline=$(($(date +%s) + 60))
    while [ $(date +%s) -lt $deadline ]; do
        if grep -q "started in" "$GEN_LOG" 2>/dev/null; then
            log "Pipeline booted."
            sleep 2
            log "Pipeline ready (pid $(cat "$GEN_PID_FILE"))."
            return
        fi
        sleep 2
    done
    warn "Pipeline did not finish booting within 60s."
}

stop_pipeline() {
    stop_rust_workers
    if [ -f "$GEN_PID_FILE" ]; then
        local pid=$(cat "$GEN_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping pipeline (pid $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$GEN_PID_FILE"
    fi
}

# ------- commands -------

cmd_test() {
    log "Running E2E integration tests (Testcontainers)..."
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
            --target) TARGET="$2"; shift 2;;
            --count) count="$2"; shift 2;;
            --interval-ms) interval="$2"; shift 2;;
            *) shift;;
        esac
    done
    validate_target

    trap 'stop_alarm_watch; stop_pipeline; stop_monitor; stop_infra' EXIT INT TERM
    start_infra
    build_all
    scaffold_and_build_pipeline
    start_monitor
    start_pipeline

    start_alarm_watch
    echo ""

    cd "$PROJECT_DIR"

    local mode="continuous (Ctrl-C to stop)"
    [ "$count" != "-1" ] && mode="bounded: $count records (interval=${interval}ms)"

    log "Starting data feed ($TARGET target) — $mode"
    log "Pipeline processes orders through: transform_order -> coerce_types -> [XOR route_by_amount] -> ..."
    echo ""

    local feed_args=("$REDPANDA_BOOTSTRAP" "$E2E_PROCESS_ID" "$interval")
    if [ "$count" != "-1" ]; then
        feed_args+=("$count")
    fi
    java -Djava.util.logging.manager=org.jboss.logmanager.LogManager \
        -cp "$TOOLS_JAR" "$FEED_PUBLISHER" "${feed_args[@]}"
}

cmd_test_run() {
    local feed_count="-1"
    local feed_interval="100"

    while [ $# -gt 0 ]; do
        case "$1" in
            --target) TARGET="$2"; shift 2;;
            --count) feed_count="$2"; shift 2;;
            --interval-ms) feed_interval="$2"; shift 2;;
            *) shift;;
        esac
    done
    validate_target

    trap 'stop_alarm_watch; stop_pipeline; stop_monitor; stop_infra' EXIT INT TERM
    start_infra

    cmd_test
    echo ""
    warn "Tests passed. Starting pipeline + monitoring + data feed."
    warn "Press Ctrl-C to stop and clean up."
    echo ""

    build_all
    scaffold_and_build_pipeline
    start_monitor
    start_pipeline

    start_alarm_watch
    echo ""
    cd "$PROJECT_DIR"

    local mode="continuous (Ctrl-C to stop)"
    [ "$feed_count" != "-1" ] && mode="bounded: $feed_count records (interval=${feed_interval}ms)"

    log "Starting data feed ($TARGET target) — $mode"
    echo ""

    local feed_args=("$REDPANDA_BOOTSTRAP" "$E2E_PROCESS_ID" "$feed_interval")
    if [ "$feed_count" != "-1" ]; then
        feed_args+=("$feed_count")
    fi
    java -Djava.util.logging.manager=org.jboss.logmanager.LogManager \
        -cp "$TOOLS_JAR" "$FEED_PUBLISHER" "${feed_args[@]}"
}

cmd_stop() {
    stop_alarm_watch
    stop_pipeline
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
