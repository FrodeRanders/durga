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

Generated bundles use `durga.gautelis.org/charlotte-v1alpha6` and pin runtime
crates to CharlotteOS `9f3d5203e4604c3113afb07e25b28ef6cd0db48c` (runtime ABI 3).
The generated build script delegates to the OS's `scripts/build-external-elf.sh`,
verifies the checkout revision, and keeps all build output in the generated
project. Freestanding x86 builds select `polyval_force_soft` to match the
platform's erasing authentication-state backend. The generated release profile
also matches the platform's abort/LTO code generation. `CHARLOTTE_ARCH` selects `aarch64` (default) or `x86_64`. Each signed
`CDEPLOY6` descriptor binds the architecture, ABI, feature mask, digest, and
object key of its ELF variant. The signer takes the final signed ELF **path**,
not a digest argument; additional architectures use explicit `--variant=` inputs.
Signing commands accept restricted private-key file paths, never key bytes.

Deployment and release commands require verified mTLS. Set
`CATTEN_MANAGEMENT_CA_FILE`, `CATTEN_MANAGEMENT_CERT_FILE`, and
`CATTEN_MANAGEMENT_KEY_FILE` to PEM files; the certificate must be enrolled as
an administrator for mutations. `CATTEN_MANAGEMENT_SERVER_NAME` optionally
supplies the certificate's DNS/IP identity. The guest must have the matching
sealed management TLS resource provisioned. These credentials are separate
from artifact/release signing keys and Kafka connector credentials.

For Durga activities, the planned default is a procedural application behind a
generic transactional-step service. The service polls Kafka, calls the
procedure, produces its result to allow-listed routes, and commits the input
offset in the same transaction. The procedure itself receives no Kafka
capability. Holding several producer/consumer service endpoints is supported
for independent work, but cannot create one transaction across those endpoints.

Each generated step declares Charlotte Kafka profile format version 7. The
deployment controller must serialize the complete broker-destination allow-list,
consume, produce, group, transaction, TLS, and rights configuration into one
SHA-256-protected object and deliver it as a kernel-enforced read-only launch
capability. The hash detects corruption; launcher authority establishes the
profile's provenance. Profile v7 has an exact 112-byte header and one closed
authentication choice: none, SCRAM-SHA-256, mTLS-P256, or both. Authentication
fields are bounded; retired v6 encodings and trailing extensions reject. The same profile carries a bounded transactional
authority-endpoint name and its consume/produce/transaction ceiling. The
connector checks that ceiling against every operation, while every deployment
step receives only that access-point capability and no broker configuration is
exposed to its procedure. Kafka
metadata may select only a provisioned destination
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
Connector instance names use the generated artifact-safe component name plus
`-connector`; the transactional access point adds `-transactional`. Neither is
the single global `kafka` name, and neither can be changed by application code.

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

The research `charlotte-kafka-broker` is not yet a complete destination for the
transactional-step runner. It still needs verified broker TLS and the
`AddOffsetsToTxn` v0 / `TxnOffsetCommit` v3 wire APIs. Updating generated
platform contracts does not establish end-to-end transactional interoperability.
