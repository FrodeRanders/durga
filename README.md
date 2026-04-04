# Durga
Durga is a BPMN-driven Kafka scaffolding tool. It reads a BPMN model and generates Kafka-oriented worker,
gateway, orchestration, and topic setup skeletons for process implementations.

In Sanskrit, "Camunda" (or "Chamunda") refers to a Hindu goddess associated with war and protection. Chamunda is depicted as a fierce aspect of the goddess Kali or Durga, often portrayed as a warrior or protector.

The project currently focuses on code generation and local process experimentation:
- Parse BPMN models with Camunda
- Generate Java skeletons with StringTemplate
- Emit Kafka topic scripts and generated-project metadata
- Support local development against the bundled Kafka setup

You'll find the [manual](doc/system/sysdoc.pdf) in `doc/system`.

## Quick start
Build the shaded JAR and run the scaffolder against a BPMN file:

```bash
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar path/to/process.bpmn
```

## Setup
Start the bundled Kafka broker and Kafka UI for local development:

```
➜  cd setup
➜  docker compose up
[+] Running 3/3
✔ Network setup_default      Created                                                                                                                  0.0s
✔ Container setup-kafka_b-1  Created                                                                                                                  0.1s
✔ Container kafka-ui         Created                                                                                                                  0.0s
Attaching to kafka-ui, kafka_b-1
kafka_b-1  | kafka 11:12:06.86 INFO  ==>
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Welcome to the Bitnami kafka container
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Subscribe to project updates by watching https://github.com/bitnami/containers
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Did you know there are enterprise versions of the Bitnami catalog? For enhanced secure software supply chain features, unlimited pulls from Docker, LTS support, or application customization, see Bitnami Premium or Tanzu Application Catalog. See https://www.arrow.com/globalecs/na/vendors/bitnami/ for more information.
kafka_b-1  | kafka 11:12:06.86 INFO  ==>
kafka_b-1  | kafka 11:12:06.86 INFO  ==> ** Starting Kafka setup **
kafka-ui   | Standard Commons Logging discovery in action with spring-jcl: please remove commons-logging.jar from classpath in order to avoid potential conflicts
kafka-ui   |  _   _ ___    __             _                _          _  __      __ _
kafka-ui   | | | | |_ _|  / _|___ _ _    /_\  _ __ __ _ __| |_  ___  | |/ /__ _ / _| |_____
kafka-ui   | | |_| || |  |  _/ _ | '_|  / _ \| '_ / _` / _| ' \/ -_) | ' </ _` |  _| / / _`|
kafka-ui   |  \___/|___| |_| \___|_|   /_/ \_| .__\__,_\__|_||_\___| |_|\_\__,_|_| |_\_\__,|
kafka-ui   |                                  |_|                                             
...
```

The 'kafka-ui' tries for a while to connect to 'kafka_b-1', before Kafka is up and running: 

```
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Node -1 disconnected.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Connection to node -1 (kafka_b/172.18.0.2:9092) could not be established. Broker may not be available.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Node -1 disconnected.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Connection to node -1 (kafka_b/172.18.0.2:9092) could not be established. Broker may not be available.
...
```

Eventually connection is made and things calms down: 

```
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka version: 4.0.0 (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka commitId: 985bc99521dd22bb (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka startTimeMs: 1746011536964 (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```

## BPMN scaffolding
Generate worker, gateway, and orchestrator skeletons from a BPMN XML file using Camunda and ST4:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar path/to/process.bpmn
```

Output is written under `generated/` by default:
- Java sources in `generated/src/main/java/se/fk/kafka/generated/`
- `topics.sh` and `summary.json` in `generated/`
- demo/test helpers such as `demo-scenario.sh`, `send-task-input.sh`, `complete-task.sh`, `fail-task.sh`, `escalate-task.sh`, `complete-call-activity.sh`, `send-message-event.sh`, `send-signal-event.sh`, `watch-process-events.sh`, and `watch-task-output.sh`
- `task-payloads.json` with sample task-shaped input payloads
- `README.md` describing generated artifacts
- `pom.xml` for building the generated process on its own

The generator skips any Java class that already exists in `src/main/java/`, merges new channels into
`src/main/resources/application.yml` (formatting/comments may change), and includes XOR conditions as
placeholders for you to implement.

Note: generated modules build a shaded `*-all.jar` so you can run starter classes without managing a classpath.

Optional flags:
- `--dry-run` prints `summary.json`, `topics.sh`, and YAML previews without writing files.
- `--out <dir>` writes generated files to a custom output directory.
- `--transactions` generates transactional worker classes using Kafka's producer/consumer APIs.

The current BPMN element support boundary is documented in [docs/bpmn-kafka-coverage.md](/Users/froran/Projects/fk/kafkaplay/docs/bpmn-kafka-coverage.md).

## Monitoring with Kafka Streams
Durga now includes a Kafka Streams monitoring skeleton built around a canonical `process-events` topic.
The topology projects:
- latest state per process instance into `process-state`
- counts by current process state into `process-state-counts`

The main entry points are:
- `org.gautelis.durga.monitoring.ProcessMonitoringTopology`
- `org.gautelis.durga.monitoring.ProcessMonitoringApp`

Run the monitoring app against the local broker:

```bash
mvn -q package
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringApp localhost:9094 durga-monitoring 8081
```

The projection model lives in `org.gautelis.durga.monitoring.ProcessStateView`, and the normalized event contract is
represented by `org.gautelis.durga.ProcessEvent`.

The monitoring app exposes a small HTTP query surface:
- `GET /dashboard`
- `GET /health`
- `GET /instances/{processInstanceId}`
- `GET /processes/{processId}/counts`
- `GET /processes/{processId}/latency`
- `GET /counts`
- `GET /stuck?processId=<id>&olderThanSeconds=60`

There is also a tiny CLI client for those endpoints:

```bash
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringClient http://localhost:8081 health
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringClient http://localhost:8081 counts invoice_receipt
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringClient http://localhost:8081 latency invoice_receipt
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringClient http://localhost:8081 stuck invoice_receipt 300
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessMonitoringClient http://localhost:8081 instance <processInstanceId>
```

Open `http://localhost:8081/dashboard` for a live dashboard with counts, latency, stuck instances, and
instance inspection.

To drive the monitor without a generated workflow, publish a demo scenario directly to `process-events`:

```bash
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessEventScenarioRunner localhost:9094 happy invoice_receipt register_invoice,review_invoice,notify_requester
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessEventScenarioRunner localhost:9094 stuck invoice_receipt register_invoice,review_invoice,notify_requester
java -cp target/durga-1.0-SNAPSHOT.jar org.gautelis.durga.monitoring.ProcessEventScenarioRunner localhost:9094 failed invoice_receipt register_invoice,review_invoice,notify_requester
```

## End-to-end demo
Run the full monitoring loop locally:

1. Start Kafka:

```bash
cd setup
docker compose up -d
```

2. Run the scripted demo:

```bash
./setup/demo-monitoring.sh
```

That script will:
- build the project
- start the monitoring app on `http://localhost:18081`
- publish a scenario to `process-events`
- query `health`, `counts`, `latency`, `stuck`, and the final instance view

You can also start Kafka from the script itself:

```bash
START_KAFKA=true ./setup/demo-monitoring.sh
SCENARIO=stuck ./setup/demo-monitoring.sh
SCENARIO=failed ./setup/demo-monitoring.sh
```

## Generated project probes
Every scaffolded process now includes producer and observer helpers so you can poke the generated graph without writing extra code:

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

The watcher helpers are useful when you want to see the canonical lifecycle stream and the per-task output topics side by side.
`complete-task.sh` is the path that advances generated BPMN user/manual tasks after they have entered a waiting state.
`fail-task.sh` is the path that emits task failures for testing boundary error handling.
`escalate-task.sh` is the path that emits escalated task events for testing boundary escalation handling.
`complete-call-activity.sh` is the path that replies to generated BPMN call activities.
`send-message-event.sh` is the path that resumes BPMN message catch events from an external Kafka topic.
`send-signal-event.sh` is the path that resumes BPMN signal catch events from an external Kafka topic.
Embedded subprocesses now generate explicit scope entry/completion services, so the lifecycle stream includes subprocess-level enter and complete events in addition to the internal task events.
Event subprocesses with message or signal start events now generate dedicated start/completion services driven by the corresponding external Kafka topic. Non-interrupting starts branch alongside the parent flow; interrupting starts emit cancellation for the enclosing scope before starting the event subprocess path. Timer, error, and escalation event subprocess starts are also supported when they are scoped inside an embedded subprocess.

### Demo BPMN model
An example BPMN model is included at `src/main/resources/bpmn/invoice_receipt.bpmn`, based on the common
"Invoice Receipt" demo process pattern (register, review, approve/reject, notify). You can generate
scaffolding from it like this:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_receipt.bpmn
```

Another common demo is the order fulfillment flow at `src/main/resources/bpmn/order_fulfillment.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/order_fulfillment.bpmn
```

A timer-based example is included at `src/main/resources/bpmn/invoice_receipt_reminder.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_receipt_reminder.bpmn
```

A message-event example is included at `src/main/resources/bpmn/invoice_message_exchange.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_message_exchange.bpmn
```

A signal-event example is included at `src/main/resources/bpmn/invoice_signal_exchange.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_signal_exchange.bpmn
```

A boundary-timer example is included at `src/main/resources/bpmn/invoice_review_deadline.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_review_deadline.bpmn
```

A non-interrupting boundary-timer example is included at `src/main/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn
```

A boundary-error example is included at `src/main/resources/bpmn/invoice_processing_error.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_processing_error.bpmn
```

A boundary-escalation example is included at `src/main/resources/bpmn/invoice_review_escalation.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_review_escalation.bpmn
```

A call-activity example is included at `src/main/resources/bpmn/invoice_call_activity.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_call_activity.bpmn
```

An embedded-subprocess example is included at `src/main/resources/bpmn/invoice_review_subprocess.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_review_subprocess.bpmn
```

A subprocess-boundary timer example is included at `src/main/resources/bpmn/invoice_subprocess_deadline.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_subprocess_deadline.bpmn
```

A non-interrupting subprocess-boundary timer example is included at `src/main/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn
```

A subprocess-boundary error example is included at `src/main/resources/bpmn/invoice_subprocess_error.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_subprocess_error.bpmn
```

A message-start event subprocess example is included at `src/main/resources/bpmn/invoice_event_subprocess_message.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_event_subprocess_message.bpmn
```

An interrupting message-start event subprocess example is included at `src/main/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn
```

A timer-start event subprocess example is included at `src/main/resources/bpmn/invoice_event_subprocess_timer.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_event_subprocess_timer.bpmn
```

An error-start event subprocess example is included at `src/main/resources/bpmn/invoice_event_subprocess_error.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_event_subprocess_error.bpmn
```

An escalation-start event subprocess example is included at `src/main/resources/bpmn/invoice_event_subprocess_escalation.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_event_subprocess_escalation.bpmn
```

A nested-subprocess example is included at `src/main/resources/bpmn/invoice_nested_subprocess.bpmn`:

```
mvn -q clean package
java -jar target/durga-1.0-SNAPSHOT.jar src/main/resources/bpmn/invoice_nested_subprocess.bpmn
```
