# Durga

![The pipeline demo, viewed in Camunda Modeler](./doc/images/data_pipeline_demo.png)

BPMN-driven Kafka scaffolding tool. Reads a BPMN model and generates Kafka-oriented
worker, gateway, orchestration, and topic setup skeletons for process implementations.

[System manual](doc/system/sysdoc.pdf) | [BPMN coverage matrix](doc/bpmn-kafka-coverage.md) | [Beta support boundary](doc/beta-support-boundary.md) | [Maturity plan](doc/maturity-plan.md) | [Release checklist](doc/release-checklist.md) | [Operations hardening](doc/operations-hardening.md) | [Deployment guide](doc/deployment.md) | [Plugin architecture](doc/data-pipeline-blueprint.md) | [Testcontainers setup](doc/testcontainers-setup.md)

## Quick start

```bash
mvn -q clean package
java -jar target/durga-0.1.0-beta.1.jar path/to/process.bpmn
```

## Local Kafka setup

```bash
cd setup && docker compose up
```

Kafka UI will log transient connection errors until the broker is ready — this is normal.
The broker listens on `localhost:9094`.

## Running tests

```bash
# Unit tests and generated-project checks (no Docker):
mvn test -Dtest='!*IntegrationTest'

# Monitoring UI tests and build:
cd monitoring-ui && npm ci && npm test && npm run build

# Integration tests (require Docker):
mvn test -Dtest='*IntegrationTest'

# If Docker detection fails, use the Linux container fallback:
./setup/run-integration-tests.sh
```

See [Testcontainers setup](doc/testcontainers-setup.md) for details.

## BPMN scaffolding

Output lands in `generated/` by default:

- Java sources in `generated/src/main/java/org/gautelis/durga/generated/`
- `topics.sh` and `summary.json`
- `task-payloads.json` with sample input payloads
- `pom.xml` and `README.md` for the generated project
- Helper scripts: `demo-scenario.sh`, `send-task-input.sh`, `complete-task.sh`,
  `fail-task.sh`, `escalate-task.sh`, `complete-call-activity.sh`,
  `send-message-event.sh`, `send-signal-event.sh`, `watch-process-events.sh`,
  `watch-task-output.sh`

Flags:

- `--dry-run` — print `summary.json`, `topics.sh`, connect configs, and application YAML without writing files
- `--out <dir>` — custom output directory
- `--event-topic <topic>` — override the canonical lifecycle event topic (default: `process-events-{processId}`). Each pipeline gets an isolated topic by default; use this flag to share a topic across pipelines or use a custom name.
- `--transactions` — generate transactional workers using Kafka producer/consumer APIs

The generator skips existing files in `src/main/java/`, merges new channels into
`application.yml`, and evaluates gateway conditions from BPMN `conditionExpression` at runtime.

## BPMN sample catalog

All sample models live under `src/test/resources/bpmn/`. Run any with:

```bash
mvn -q clean package
java -jar target/durga-0.1.0-beta.1.jar src/test/resources/bpmn/<model>.bpmn
```

| Model | Feature |
|-------|---------|
| `invoice_receipt.bpmn` | Baseline process (start, service, review, approve/reject, notify) |
| `order_fulfillment.bpmn` | Legacy reference model |
| `invoice_receipt_reminder.bpmn` | Intermediate timer catch |
| `invoice_message_exchange.bpmn` | Message throw/catch |
| `invoice_signal_exchange.bpmn` | Signal throw/catch |
| `invoice_review_deadline.bpmn` | Interrupting timer boundary |
| `invoice_review_reminder_non_interrupting.bpmn` | Non-interrupting timer boundary |
| `invoice_processing_error.bpmn` | Interrupting error boundary |
| `invoice_review_escalation.bpmn` | Interrupting escalation boundary |
| `invoice_call_activity.bpmn` | Call activity request/reply |
| `invoice_review_subprocess.bpmn` | Embedded subprocess |
| `invoice_nested_subprocess.bpmn` | Nested subprocesses |
| `invoice_subprocess_deadline.bpmn` | Interrupting timer boundary on subprocess |
| `invoice_subprocess_reminder_non_interrupting.bpmn` | Non-interrupting timer boundary on subprocess |
| `invoice_subprocess_error.bpmn` | Interrupting error boundary on subprocess |
| `invoice_event_subprocess_message.bpmn` | Non-interrupting message-start event subprocess |
| `invoice_event_subprocess_interrupting_message.bpmn` | Interrupting message-start event subprocess |
| `invoice_event_subprocess_timer.bpmn` | Timer-start event subprocess |
| `invoice_event_subprocess_error.bpmn` | Error-start event subprocess |
| `data_pipeline_demo.bpmn` | Plugin-annotated pipeline (json-transform, field-filter, kv-enricher) |
| `order_events_pipeline.bpmn` | 8-plugin order pipeline with XOR gateway; use `--connect` for source/sink |
| `log_processing_pipeline.bpmn` | Regex, template, flatten, validate, mask; use `--connect` |
| `custom_activity_demo.bpmn` | Custom activity with contract interface + delegating worker |

## Monitoring

A Kafka Streams topology consumes the canonical `process-events` topic and materializes:

- latest state per instance into `process-state`
- counts by state into `process-state-counts`
- active-instance index into `process-active-state`
- activity latency summaries into `process-latency`
- coarse lifecycle trend buckets into `process-trends`

### Start the monitoring app

```bash
java -cp target/durga-0.1.0-beta.1.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringApp \
  localhost:9094 durga-monitoring 8081
```

### HTTP endpoints

- `GET /dashboard`
- `GET /health`
- `GET /instances/{processInstanceId}`
- `GET /processes/{processId}/counts`
- `GET /processes/{processId}/latency`
- `GET /processes/{processId}/trends`
- `GET /counts`
- `GET /stuck?processId=<id>&olderThanSeconds=60`

### CLI client

```bash
java -cp target/durga-0.1.0-beta.1.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  http://localhost:8081 health

java -cp target/durga-0.1.0-beta.1.jar \
  org.gautelis.durga.monitoring.ProcessMonitoringClient \
  http://localhost:8081 counts invoice_receipt
```

### Demo scenarios (without a generated process)

```bash
java -cp target/durga-0.1.0-beta.1.jar \
  org.gautelis.durga.demo.ProcessEventScenarioRunner \
  localhost:9094 happy invoice_receipt register_invoice,review_invoice,notify_requester
```

### End-to-end demo script

```bash
./setup/demo-monitoring.sh
START_KAFKA=true ./setup/demo-monitoring.sh
SCENARIO=stuck ./setup/demo-monitoring.sh
SCENARIO=failed ./setup/demo-monitoring.sh
```

### Docker demo (continuous feed + monitoring UI)

```bash
docker compose -f setup/docker-compose.demo.yml up --build
```

Starts Kafka, the monitoring app with the Svelte dashboard, and a continuous
feed publisher that exercises the monitoring topology. Open
`http://localhost:8081` for the dashboard, `http://localhost:8080` for
Kafka UI.

The feed publishes a complete lifecycle every second with randomised
processing times, producing live counts, latency, and stuck-instance data.

```bash
# Customise feed parameters
FEED_PROCESS_ID=order_fulfillment FEED_INTERVAL_MS=2000 \
  docker compose -f setup/docker-compose.demo.yml up --build
```

## Generated project probes

Every scaffolded project includes producer and observer helpers:

```bash
./generated/demo-scenario.sh happy
./generated/send-task-input.sh register_invoice
./generated/complete-task.sh approve_invoice <instance-id>
./generated/fail-task.sh register_invoice <instance-id>
./generated/escalate-task.sh review_invoice <instance-id>
./generated/complete-call-activity.sh validate_invoice_process <instance-id>
./generated/send-message-event.sh invoice_review_response_message <instance-id>
./generated/send-signal-event.sh invoice_review_signal_signal <instance-id>
./generated/watch-process-events.sh
./generated/watch-task-output.sh register_invoice
```

Embedded subprocesses generate scope entry/completion services. Event subprocesses with
message or signal starts generate start/completion services driven by external Kafka topics.
Interrupting starts emit cancellation for the enclosing scope; non-interrupting starts
branch alongside the parent flow. Timer, error, and escalation event subprocess starts
are supported within embedded subprocesses.
