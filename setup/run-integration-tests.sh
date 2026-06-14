#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Run Durga integration tests inside a Linux container.
# Uses Docker-out-of-Docker: mounts the host socket and reaches
# sibling containers via host.docker.internal.
#
# Prerequisites: Docker running.
# ------------------------------------------------------------------

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MAVEN_IMAGE="maven:3.9-eclipse-temurin-21"
MAVEN_CACHE="$HOME/.m2"

# --- find Docker socket --------------------------------------------------
find_socket() {
    if [ -n "${DOCKER_HOST:-}" ]; then
        local host="${DOCKER_HOST}"
        if [[ "$host" == unix://* ]]; then
            echo "${host#unix://}"
            return
        fi
    fi
    for candidate in \
        /var/run/docker.sock \
        "$HOME/.docker/run/docker.sock" \
        "$HOME/Library/Containers/com.docker.docker/Data/docker.raw.sock" \
        /run/docker.sock; do
        if [ -S "$candidate" ]; then
            echo "$candidate"
            return
        fi
    done
    echo ""
}

SOCKET="$(find_socket)"
if [ -z "$SOCKET" ]; then
    echo "ERROR: Cannot find Docker socket."
    exit 1
fi
echo "Using Docker socket: $SOCKET"

TESTS="${1:-'*IntegrationTest'}"
echo "Test pattern: $TESTS"
echo ""

# --- run ------------------------------------------------------------------
# --add-host makes host.docker.internal resolvable to the Docker host,
# so the test can reach Kafka containers mapped on the host.
# On Docker Desktop / Linux, the Docker host is reachable at the default
# gateway address from inside containers. We pass this to the test so
# it can reach Kafka containers whose ports are mapped on the host.
HOST_IP=$(docker run --rm alpine:latest sh -c 'ip route show default | awk "/default/ {print \$3}"' 2>/dev/null || echo "172.17.0.1")

exec docker run --rm \
    --mount "type=bind,src=$SOCKET,dst=/var/run/docker.sock" \
    --mount "type=bind,src=$PROJECT_DIR,dst=/project" \
    --mount "type=bind,src=$MAVEN_CACHE,dst=/root/.m2" \
    -w /project \
    -e DOCKER_HOST=unix:///var/run/docker.sock \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    -e DURGA_DOCKER_HOST_IP="$HOST_IP" \
    -e MAVEN_OPTS="-XX:+UseParallelGC" \
    "$MAVEN_IMAGE" \
    mvn test -Dtest="$TESTS" \
        -DDOCKER_HOST=unix:///var/run/docker.sock \
        -DTESTCONTAINERS_RYUK_DISABLED=true
