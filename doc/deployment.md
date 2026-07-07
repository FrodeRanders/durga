# Deployment

## Generated Project Artifacts

Every scaffolded project includes deployment-ready artifacts:

```
generated/
├── Dockerfile              # Multi-stage JRE image
├── k8s.yml                 # Kubernetes Deployment + Service
├── deploy.sh               # Build, Docker, K8s script
├── run-local.sh            # Local Kafka + profile-aware startup
├── src/main/resources/
│   └── application.yml     # Profile-aware config (env var overrides)
├── topics.sh               # Kafka topic provisioning
└── pom.xml                 # Shaded JAR build
```

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `HTTP_PORT` | `8080` | Quarkus HTTP port |
| `PROFILE` | `dev` | Quarkus profile (`dev`, `prod`) |

### Quarkus profiles

The generated `application.yml` uses Quarkus profile-aware config:

```yaml
# Sets default Kafka bootstrap via env var
kafka:
  bootstrap:
    servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}

# Profile-specific overrides in application.yml:
"%prod":
  quarkus:
    log:
      level: WARN
    http:
      port: ${HTTP_PORT:8080}
```

Override at runtime:
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka-prod:9092 PROFILE=prod ./run-local.sh
```

## Offline Development Loop

1. **Model** the pipeline in BPMN
2. **Scaffold**: `java -jar durga.jar pipeline.bpmn`
3. **Develop**: edit task business logic in the generated workers
4. **Test locally**: `START_KAFKA=true ./run-local.sh`
5. **Observe**: monitoring dashboard or `watch-process-events.sh`

## Packaging

### JAR deployment

```bash
# Build the shaded JAR (all deps bundled)
mvn clean package -DskipTests
java -jar target/<project>-all.jar localhost:9094
```

### Docker deployment

```bash
# In the generated project directory:
DOCKER=true ./deploy.sh

# Or manually:
mvn clean package -DskipTests
docker build -t my-pipeline:latest .
docker run \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  my-pipeline:latest
```

### Kubernetes deployment

```bash
# Build Docker image and apply manifests
DOCKER=true K8S=true KAFKA_BOOTSTRAP=kafka-broker:9092 ./deploy.sh

# Or manually:
kubectl apply -f k8s.yml
```

The generated `k8s.yml` deploys:
- A **Deployment** with 2 replicas, resource limits (128Mi/512Mi memory, 100m/500m CPU)
- A **Service** on port 8080
- Exec-based **liveness** and **readiness** probes

## Scaling

Generated workers use Kafka consumer groups. To scale a pipeline:

```bash
# Scale the Deployment
kubectl scale deployment/<processId> --replicas=4

# Kafka partitions determine max parallelism per consumer group.
# Configure topic partitions in topics.sh before provisioning.
```

## Topic Provisioning

Topics are created once per environment:

```bash
# Create topics (modify partitions/replication as needed)
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092 ./topics.sh
```

For production, consider declarative topic management via Strimzi `KafkaTopic` CRDs or Terraform.

## Monitoring in Production

The Durga monitoring topology (`ProcessMonitoringApp`) is a separate deployment that
consumes the `process-events-*` family. In production:

1. Deploy the monitoring app alongside the pipeline
2. Or point it at the same Kafka cluster from a separate environment
3. The Quarkus REST API + Svelte dashboard provide operational visibility

The monitoring topology uses global tables — every instance has a full replica.
For multi-instance deployments, all instances answer queries identically.

## Health Checks

The generated Docker image uses process-based health checks (`pgrep`).
For custom health endpoints, add `quarkus-smallrye-health` to the generated `pom.xml`
and implement `@Liveness`/`@Readiness` annotated resources.
