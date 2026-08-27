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

## CharlotteOS process bundles

Generate the experimental Charlotte target with:

```bash
java -jar durga-tools/target/durga-tools-0.1.0-beta.1.jar \
  process.bpmn --target charlotte --out generated-charlotte
```

This produces a portable `no_std` Rust activity-contract crate, the original
BPMN model, and three descriptors:

- `charlotte/bundle.yaml` is the versioned semantic/process bundle;
- `charlotte/deployment.yaml` declares CLS2 artifacts, placement, rollout, and
  current single-node manifest adaptation; and
- `charlotte/capabilities.yaml` separates procedure authority from the generic
  Kafka transactional-step service's broker authority.

The output is deliberately marked non-deployable while generated handlers or
required platform contracts are missing. A trusted later stage must compile
self-contained AArch64 ELFs, attach CLS2 metadata, sign them, compute artifact
and provenance digests, resolve capability profiles, select nodes, and submit
generation-fenced assignments to the Charlotte cluster.

For Durga activities, the planned default is a procedural application behind a
generic transactional-step service. The service polls Kafka, calls the
procedure, produces its result to allow-listed routes, and commits the input
offset in the same transaction. The procedure itself receives no Kafka
capability. Holding several producer/consumer service endpoints is supported
for independent work, but cannot create one transaction across those endpoints.

Each generated step declares Charlotte Kafka profile format version 3. The
deployment controller must serialize the complete broker-destination allow-list,
consume, produce, group, transaction, TLS, and rights configuration into one
SHA-256-protected object and deliver it as a kernel-enforced read-only launch
capability. The hash detects corruption; launcher authority establishes the
profile's provenance. Kafka metadata may select only a provisioned destination
whose advertised hostname and port match exactly. Generated plans select safety
ceilings of 32 broker endpoints and 64 produce routes; a site may lower those
values, and Charlotte rejects profiles whose declared or actual counts exceed
its hard ceilings.

The Kafka connector and the transactional-step runner are separate deployment
authorities. Only the connector receives broker addresses, TLS material, and
SASL/SCRAM or mTLS secrets. The step runner receives a connection capability
to that connector plus a connection to the activity procedure; the procedure
receives neither Kafka authority nor profile material. Connector credentials
can therefore be rotated without rebuilding generated business logic.

CharlotteOS now implements this generic runner as `kafka_step.elf` with the
bounded `charlotte-kafka-step` procedure ABI. It validates all returned route
indices before starting a transaction, combines admitted outputs with the
input offset, retries procedure timeouts/transient replies, and transactionally
writes terminal, malformed, or exhausted records to the configured DLQ.
Generated deployment plans therefore mark the runner contract as available.
They remain non-deployable until activity handlers exist and the Charlotte
controller injects the exact connector and procedure capabilities, signs the
artifacts, and realizes placement.

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
