#!/usr/bin/env bash
set -euo pipefail

# Companion to scripts/run-e2e-test.sh.
#
# Assuming `run-e2e-test.sh feed` (or `test-run`) is already running — i.e. the
# Redpanda infra, the monitoring server, the production e2e_pipeline, and the
# data feed are all up — this starts a VALIDATION run of the same process:
# a complete shadow that reads the same live input topics (through a dedicated
# consumer group, so it never steals from production), runs the candidate with
# substantial side effects suppressed, and writes only to "-validation" topics.
# The monitor then compares the candidate against production per task.
#
# By default the candidate is the SAME e2e_pipeline.bpmn, which is a useful
# smoke test (comparisons should be EQUAL). Point --bpmn at a modified model to
# validate a real follow-up release before promoting it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

REDPANDA_BOOTSTRAP="localhost:9094"
REDPANDA_CONTAINER="durga-redpanda"
MONITOR_PORT="${MONITOR_PORT:-8081}"
SCAFFOLDER="org.gautelis.durga.tools.BpmnScaffolder"
TOOLS_JAR="$PROJECT_DIR/durga-tools/target/durga-tools-0.1.0-beta.1.jar"

E2E_BPMN_PATH="$PROJECT_DIR/durga-tools/src/test/resources/bpmn/e2e_pipeline.bpmn"
E2E_PROCESS_ID=$(grep -o 'bpmn:process[^>]*id="[^"]*"' "$E2E_BPMN_PATH" | grep -o 'id="[^"]*"' | head -1 | cut -d'"' -f2)
[ -z "$E2E_PROCESS_ID" ] && E2E_PROCESS_ID="e2e_pipeline"

GEN_DIR="$PROJECT_DIR/target/e2e-gen-validation"
VAL_LOG="$PROJECT_DIR/target/validation.log"
VAL_PID_FILE="$PROJECT_DIR/target/validation.pid"
VAL_RUST_PID_FILE="$PROJECT_DIR/target/validation-rust-workers.pids"
DURGA_RUST_DIR="$PROJECT_DIR/durga-rust"
DURGA_RUST_BUILD_DIR="$PROJECT_DIR/durga-rust-build"

# Candidate defaults: same process, same target as production, "candidate" label,
# and live-concurrent replay (latest). Override via flags / env.
TARGET="java"
CANDIDATE_BPMN="$E2E_BPMN_PATH"
CANDIDATE_VERSION="${DURGA_VALIDATION_CANDIDATE_VERSION:-candidate}"
OFFSET_RESET="${DURGA_VALIDATION_OFFSET_RESET:-latest}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[validation]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[err]${NC} $*" >&2; }

usage() {
    cat <<EOF
Usage: run-e2e-validation.sh [command] [options]

Runs a validation (shadow) of e2e_pipeline alongside a running
'run-e2e-test.sh feed' / 'test-run'.

Commands:
  start   Scaffold + build + run the validation shadow (default). Blocks;
          Ctrl-C stops the shadow (leaves infra/monitor/production running).
  stop    Stop a previously started validation shadow.

Options (with 'start'):
  --target T             java (default) or rust. The candidate MAY differ from the
                         production target — java and rust use identical chaining
                         topics, so a rust shadow validates a java production (and
                         vice versa) across every task.
  --bpmn PATH            Candidate BPMN model (default: the same e2e_pipeline.bpmn).
  --candidate-version V  Label for this candidate (default: 'candidate').
  --offset-reset R       Kafka auto.offset.reset for the shadow's dedicated
                         consumer group: 'latest' (default, live-concurrent) or
                         'earliest' (replay recent history).

Environment:
  MONITOR_PORT                    Monitor HTTP port (default 8081)
  DURGA_VALIDATION_OFFSET_RESET   Same as --offset-reset
  DURGA_VALIDATION_CANDIDATE_VERSION  Same as --candidate-version

Examples:
  # Terminal 1:
  ./scripts/run-e2e-test.sh feed
  # Terminal 2:
  ./scripts/run-e2e-validation.sh                 # smoke test: candidate == production
  ./scripts/run-e2e-validation.sh --bpmn /path/to/e2e_pipeline_v2.bpmn
  ./scripts/run-e2e-validation.sh --target rust --offset-reset earliest
  ./scripts/run-e2e-validation.sh stop

Watch results while it runs:
  curl -s "http://localhost:${MONITOR_PORT}/api/validation/summary?processId=${E2E_PROCESS_ID}" | jq
EOF
}

# ------- preflight -------

preflight() {
    if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${REDPANDA_CONTAINER}$"; then
        err "Redpanda container '${REDPANDA_CONTAINER}' is not running."
        err "Start production first:  ./scripts/run-e2e-test.sh feed"
        exit 1
    fi
    if ! curl -s "http://localhost:${MONITOR_PORT}/api/health" | grep -q RUNNING; then
        err "Monitoring server not healthy on port ${MONITOR_PORT}."
        err "Start production first:  ./scripts/run-e2e-test.sh feed"
        exit 1
    fi
    if ! rpk_topic_exists "${E2E_PROCESS_ID}_start"; then
        warn "Production input topic '${E2E_PROCESS_ID}_start' not found — is the feed running?"
        warn "The shadow needs live production input to compare against."
    fi
    log "Preflight OK (redpanda up, monitor healthy)."
}

rpk_topic_exists() {
    docker exec "$REDPANDA_CONTAINER" rpk topic list 2>/dev/null | awk '{print $1}' | grep -qx "$1"
}

rpk_create_topics() {
    [ "$#" -eq 0 ] && return 0
    docker exec "$REDPANDA_CONTAINER" rpk topic create "$@" 2>/dev/null || true
}

# ------- build -------

ensure_build() {
    if [ ! -f "$TOOLS_JAR" ] \
       || [ -n "$(find "$PROJECT_DIR/durga-runtime/src" "$PROJECT_DIR/durga-tools/src" -newer "$TOOLS_JAR" -print -quit 2>/dev/null)" ]; then
        log "Building durga-runtime + durga-tools (installing to local repo)..."
        ( cd "$PROJECT_DIR" && mvn -q install -DskipTests -pl durga-runtime,durga-tools )
    fi
    if [ ! -f "$TOOLS_JAR" ]; then
        err "Tools jar not found after build: $TOOLS_JAR"
        exit 1
    fi
}

# ------- java candidate -------

scaffold_java() {
    log "Scaffolding validation shadow of ${E2E_PROCESS_ID} (java) from $(basename "$CANDIDATE_BPMN")..."
    rm -rf "$GEN_DIR"
    java -cp "$TOOLS_JAR" "$SCAFFOLDER" \
        "$CANDIDATE_BPMN" --out "$GEN_DIR" --process-id "$E2E_PROCESS_ID" --validation

    # Drop generated runtime contracts that durga-runtime provides as a dependency
    # (duplicate-class / version-skew otherwise). Keep ModelEnricher + ModelRegistration.
    local core_dir="$GEN_DIR/src/main/java/org/gautelis/durga"
    if [ -d "$core_dir" ]; then
        for f in ProcessEvent ProcessState ProcessStateStore ScopeCancellationRegistry \
                 VannakMetadata DataHandle DataIndividualMetadataEvent; do
            rm -f "$core_dir/${f}.java"
        done
        rm -rf "$core_dir/plugins" "$core_dir/validation"
    fi

    log "Building validation shadow..."
    ( cd "$GEN_DIR" && mvn -q package -DskipTests )
    local jar; jar=$(echo "$GEN_DIR"/target/*-runner.jar)
    [ -f "$jar" ] || { err "Validation runner jar not found."; exit 1; }
    log "Validation shadow built: $jar"
}

# Creates the topics the validation shadow writes to.
#
# One topic is IDENTICAL for both targets and is the only one the monitor needs:
#   process-events-<pid>-validation   (ACTIVITY_COMPLETED events → compared vs production)
#
# The remaining, per-task topics legitimately DIFFER by target, because the two
# code generators chain through different intermediate topics:
#   - java: each task writes its own  <pid>_<task>_output(-validation)
#   - rust: each task forwards to the next task's  <pid>_<next>_in(-validation)
# In validation mode these are internal/orphaned (each shadow task reads live
# PRODUCTION input), so they only need to exist to avoid producer errors on a
# broker with auto-create disabled.
create_validation_topics() {
    local shared="process-events-${E2E_PROCESS_ID}-validation"
    local extra=()

    if [ "$TARGET" = "rust" ]; then
        # Rust now uses the same topic naming as java. Bins don't emit a topics.sh, so create the
        # aligned superset per task/gateway: its output stream, its (gateway-fed) inbox, and its DLQ.
        local f name
        for f in "$GEN_DIR"/src/bin/*.rs; do
            name=$(basename "$f" .rs)
            extra+=("${E2E_PROCESS_ID}_${name}_output-validation" \
                    "${E2E_PROCESS_ID}_${name}_input-validation" \
                    "${E2E_PROCESS_ID}_${name}_dlq-validation")
        done
    else
        # The generated validation topics.sh is authoritative for the java target;
        # it already includes the shared events topic and every per-task topic.
        if [ -f "$GEN_DIR/topics.sh" ]; then
            while IFS= read -r t; do extra+=("$t"); done \
                < <(grep -hoE '"[^"]+-validation"' "$GEN_DIR/topics.sh" | tr -d '"' | sort -u)
        fi
    fi

    log "Creating validation topics (shared: ${shared}; ${#extra[@]} target-specific)..."
    rpk_create_topics "$shared" ${extra[@]+"${extra[@]}"}
}

run_java() {
    local jar; jar=$(echo "$GEN_DIR"/target/*-runner.jar)
    : > "$VAL_LOG"
    log "Starting validation shadow (candidate='${CANDIDATE_VERSION}', offset-reset='${OFFSET_RESET}')."
    log "Reading live production input via consumer group '${E2E_PROCESS_ID}-validation'; writing only to -validation topics."
    log "Logs -> $VAL_LOG   |   Ctrl-C to stop."
    echo ""
    DURGA_VALIDATION_CANDIDATE_VERSION="$CANDIDATE_VERSION" \
    DURGA_VALIDATION_OFFSET_RESET="$OFFSET_RESET" \
    exec java -Dkafka.bootstrap.servers="$REDPANDA_BOOTSTRAP" \
         -Dquarkus.http.port=0 \
         -jar "$jar"
}

# ------- rust candidate -------

scaffold_rust() {
    command -v cargo >/dev/null 2>&1 || { err "cargo not found — install a Rust toolchain for --target rust."; exit 1; }
    log "Scaffolding validation shadow of ${E2E_PROCESS_ID} (rust) from $(basename "$CANDIDATE_BPMN")..."
    rm -rf "$GEN_DIR"
    java -Ddurga.rust.crate.path="$DURGA_RUST_DIR" -Ddurga.rust.build.crate.path="$DURGA_RUST_BUILD_DIR" \
        -cp "$TOOLS_JAR" "$SCAFFOLDER" \
        "$CANDIDATE_BPMN" --out "$GEN_DIR" --process-id "$E2E_PROCESS_ID" --target rust --validation
    log "Building validation shadow (cargo build --release)..."
    ( cd "$GEN_DIR" && cargo build --release ) || { err "cargo build failed."; exit 1; }
    log "Validation shadow built."
}

run_rust() {
    : > "$VAL_RUST_PID_FILE"
    : > "$VAL_LOG"
    log "Starting validation shadow rust workers (candidate='${CANDIDATE_VERSION}', offset-reset='${OFFSET_RESET}')."
    log "Logs -> $VAL_LOG   |   Ctrl-C to stop."
    local f name bin count=0
    for f in "$GEN_DIR"/src/bin/*.rs; do
        name=$(basename "$f" .rs)
        bin="$GEN_DIR/target/release/$name"
        [ -x "$bin" ] || { warn "  missing binary: $bin"; continue; }
        KAFKA_BROKERS="$REDPANDA_BOOTSTRAP" \
        DURGA_VALIDATION_CANDIDATE_VERSION="$CANDIDATE_VERSION" \
        DURGA_VALIDATION_OFFSET_RESET="$OFFSET_RESET" \
        "$bin" >> "$VAL_LOG" 2>&1 &
        echo $! >> "$VAL_RUST_PID_FILE"
        log "  started shadow worker: $name (pid $!)"
        count=$((count + 1))
    done
    log "Validation shadow ready ($count worker process(es))."
    trap 'stop_validation; exit 0' INT TERM
    wait
}

# ------- lifecycle -------

stop_validation() {
    if [ -f "$VAL_PID_FILE" ]; then
        local pid; pid=$(cat "$VAL_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping validation shadow (pid $pid)..."
            kill "$pid" 2>/dev/null || true; sleep 2; kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$VAL_PID_FILE"
    fi
    if [ -f "$VAL_RUST_PID_FILE" ]; then
        log "Stopping validation shadow rust workers..."
        local pid
        while read -r pid; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done < "$VAL_RUST_PID_FILE"
        sleep 1
        while read -r pid; do [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true; done < "$VAL_RUST_PID_FILE"
        rm -f "$VAL_RUST_PID_FILE"
    fi
}

cmd_start() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --target) TARGET="$2"; shift 2;;
            --bpmn) CANDIDATE_BPMN="$2"; shift 2;;
            --candidate-version) CANDIDATE_VERSION="$2"; shift 2;;
            --offset-reset) OFFSET_RESET="$2"; shift 2;;
            -h|--help) usage; exit 0;;
            *) warn "ignoring unknown option: $1"; shift;;
        esac
    done

    if [ "$TARGET" != "java" ] && [ "$TARGET" != "rust" ]; then
        err "--target must be 'java' or 'rust' (got '$TARGET')"; exit 1
    fi
    if [ ! -f "$CANDIDATE_BPMN" ]; then
        err "Candidate BPMN not found: $CANDIDATE_BPMN"; exit 1
    fi

    preflight
    ensure_build

    if [ "$TARGET" = "rust" ]; then
        scaffold_rust
    else
        scaffold_java
    fi
    create_validation_topics

    echo ""
    log "The monitor discovers the validation events topic on its next metadata refresh;"
    log "results appear at:  http://localhost:${MONITOR_PORT}  (Validation Report panel)"
    log "or:  curl -s 'http://localhost:${MONITOR_PORT}/api/validation/summary?processId=${E2E_PROCESS_ID}'"
    echo ""

    if [ "$TARGET" = "rust" ]; then
        run_rust
    else
        # Foreground: Ctrl-C stops the shadow directly.
        run_java
    fi
}

# ------- main -------

case "${1:-}" in
    ""|start)   shift 2>/dev/null || true; cmd_start "$@";;
    stop)       stop_validation;;
    -h|--help)  usage;;
    --*)        cmd_start "$@";;   # options-first: default to 'start'
    *)          usage; exit 1;;
esac
