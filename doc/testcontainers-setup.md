# Testcontainers Setup

Integration tests use [Testcontainers](https://testcontainers.com) to start a real Kafka broker
(and optionally Toxiproxy) in Docker containers.

## Prerequisites

- Docker installed and running (Docker Desktop on macOS, or docker-ce on Linux)
- `docker info` must succeed without `sudo`

## How Docker is detected

The test infrastructure automatically discovers the Docker socket by probing these
locations in order:

1. `DOCKER_HOST` environment variable
2. `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` environment variable
3. `/var/run/docker.sock` (Linux default, macOS Docker Desktop symlink)
4. `$HOME/.docker/run/docker.sock` (macOS Docker Desktop)
5. `$HOME/Library/Containers/com.docker.docker/Data/docker.raw.sock` (macOS alternative)
6. `/run/docker.sock`

If none of these work, the integration tests are skipped gracefully with a
descriptive message.

## Manual configuration

If auto-detection fails, configure the socket explicitly.

### Option A: set DOCKER_HOST

```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

Add to your shell profile (`~/.zshrc`, `~/.bashrc`) for persistence.

### Option B: testcontainers.properties

Create or edit `~/.testcontainers.properties`:

```properties
docker.host=unix:///Users/yourname/.docker/run/docker.sock
```

Replace `/Users/yourname` with the output of `echo $HOME`.

### Option C: TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE

```bash
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=$HOME/.docker/run/docker.sock
```

## Verify setup

```bash
# Check Docker works
docker run --rm hello-world

# Run a quick integration test
mvn test -Dtest='ProcessEventKafkaIntegrationTest'
```

If the test is skipped with "Docker not available", re-check `docker info` and
the configuration options above.

## Colima users

If you use [Colima](https://github.com/abiosoft/colima) instead of Docker Desktop:

```bash
colima start
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
```

Add the export line to your shell profile.

## Remote Docker (DOCKER_HOST=tcp://...)

Testcontainers works with TCP Docker hosts when TLS certificates are configured:

```bash
export DOCKER_HOST=tcp://192.168.1.10:2375
export DOCKER_TLS_VERIFY=1
export DOCKER_CERT_PATH=$HOME/.docker
```

## Troubleshooting

**"Could not find a valid Docker environment"**

1. Run `docker info` — if it fails, start Docker first
2. Run `echo $DOCKER_HOST` — if set, verify the socket path exists
3. Create `~/.testcontainers.properties` with the correct `docker.host=`
4. Check file permissions: `ls -la $(echo $HOME)/.docker/run/docker.sock`

**Docker Desktop on macOS**

If Docker Desktop doesn't create `/var/run/docker.sock`, enable it in
Docker Desktop → Settings → Advanced → "Allow the default Docker socket to be used".

**Testcontainers version conflict**

The project pins Testcontainers 1.21.3 and docker-java 3.7.1 via the
`httpclient5` transport (for Apple Silicon compatibility). If you see
`NoClassDefFoundError` for docker-java classes, run:

```bash
mvn dependency:tree -DincludeScope=test | grep docker-java
```

Ensure `docker-java-transport-httpclient5:3.7.1` appears and
`docker-java-transport-zerodep` does NOT appear.

**Ryuk container fails to start**

Add this to `~/.testcontainers.properties`:

```properties
ryuk.container.privileged=true
```
