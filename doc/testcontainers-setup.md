# Testcontainers Setup

Integration tests use [Testcontainers](https://testcontainers.com) 2.0.5 to start a
real Kafka broker in Docker containers.

## Prerequisites

- Docker installed and running (Docker Desktop on macOS, or docker-ce on Linux)
- `docker info` must succeed without `sudo`

## Quick start

```bash
mvn test -Dtest='*IntegrationTest'
```

Testcontainers 2.0.5 auto-detects the Docker socket on both macOS and Linux —
no manual configuration needed. Ryuk (the container reaper) runs automatically
and cleans up containers after each test run.

If a release or CI gate must fail when Docker is unavailable, require Docker
explicitly:

```bash
mvn test -Dtest='*IntegrationTest' -Ddurga.integration.requireDocker=true
```

The same behavior can be enabled with `DURGA_REQUIRE_DOCKER_TESTS=true`.

## Running inside a Linux container (fallback)

If auto-detection fails on your platform, run the tests inside a Linux container
that mounts the Docker socket:

```bash
./setup/run-integration-tests.sh                      # all integration tests
./setup/run-integration-tests.sh ChaosIntegrationTest  # single test class
```

This script uses a `maven:3.9-eclipse-temurin-21` image and disables Ryuk
(`TESTCONTAINERS_RYUK_DISABLED=true`) because Ryuk cannot bind its control port
through the Docker-out-of-Docker network layer.

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
mvn test -Dtest='*IntegrationTest'
```

**"Timed out waiting for a node assignment"**

Kafka broker started but the client can't reach it. The test infrastructure
picks a random free host port per test class (mapped to the container's internal
port 9092), falling back to a fixed port only if allocation fails. To inspect or
free a port, find the mapped port from the test logs, or:

```bash
lsof -i :<mapped-port>
```

**Ryuk container fails**

The container fallback disables Ryuk because it cannot reach the host gateway.
On normal host runs Ryuk is enabled and handles cleanup automatically.
If you need to disable it explicitly on the host, add to `~/.testcontainers.properties`:

```properties
ryuk.container.privileged=true
```
