# Testcontainers Setup

Integration tests use [Testcontainers](https://testcontainers.com) 2.0.2 to start a
real Kafka broker in Docker containers.

## Prerequisites

- Docker installed and running (Docker Desktop on macOS, or docker-ce on Linux)
- `docker info` must succeed without `sudo`

## Quick start

```bash
# With Ryuk disabled (avoids container-in-container issues):
mvn test -Dtest='*IntegrationTest' -DTESTCONTAINERS_RYUK_DISABLED=true

# Or pass via environment:
TESTCONTAINERS_RYUK_DISABLED=true mvn test -Dtest='*IntegrationTest'
```

Testcontainers 2.0.2 auto-detects the Docker socket on both macOS and Linux — no
manual configuration needed.

## Running inside a Linux container (fallback)

If auto-detection fails on your platform, run the tests inside a Linux container
that mounts the Docker socket:

```bash
./setup/run-integration-tests.sh                      # all integration tests
./setup/run-integration-tests.sh ChaosIntegrationTest  # single test class
```

This uses a `maven:3.9-eclipse-temurin-21` image with the project directory,
Docker socket, and `~/.m2` cache bind-mounted. It disables Ryuk and auto-detects
the Docker host IP for correct network routing between the test container and
the Kafka containers it spawns.

## Colima users

```bash
colima start
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
```

## Remote Docker (DOCKER_HOST=tcp://...)

```bash
export DOCKER_HOST=tcp://192.168.1.10:2375
export DOCKER_TLS_VERIFY=1
export DOCKER_CERT_PATH=$HOME/.docker
```

## Troubleshooting

**"Docker not available, skipping integration tests"**

Testcontainers couldn't find Docker. Check:
1. `docker info` succeeds
2. Docker Desktop is running (macOS) or docker service is active (Linux)

On macOS, if the socket is at a non-standard location:

```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
mvn test -Dtest='*IntegrationTest' -DTESTCONTAINERS_RYUK_DISABLED=true
```

**"Timed out waiting for a node assignment"**

Kafka broker started but the client can't reach it. The test infrastructure
uses a fixed host port (19092) mapped to the container. If that port is in use,
change `KAFKA_PORT` in `KafkaIntegrationTestBase` or kill the process holding it:

```bash
lsof -i :19092
```

**Ryuk container fails**

Tests disable Ryuk by default (`TESTCONTAINERS_RYUK_DISABLED=true`). If you
need Ryuk, add to `~/.testcontainers.properties`:

```properties
ryuk.container.privileged=true
```
