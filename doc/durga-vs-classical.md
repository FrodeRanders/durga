# Making the case for Durga over Airflow

This document is written to help you decide whether Durga is right for you.
A robust alternative would be Apache Airflow, so the argumentation takes Airflow
as a starting point. I will assume you know what a data pipeline is and
I guess you have opinions about how to build one.

---

## The argument in three paragraphs

**Airflow is a scheduler that runs code.** You write Python DAGs, Airflow
decides when each task runs, and workers execute them. This works fine at
modest scale, but the scheduler is a central bottleneck, the DAGs are
bespoke code you must maintain forever, and monitoring requires bolt-on
tooling.

**Durga is a compiler that produces a distributed runtime.** You draw a
pipeline as a BPMN diagram. Durga reads it and generates a set of
independent Kafka services — one per pipeline step — that execute
continuously, event by event, with no central scheduler. Change the
diagram, regenerate, redeploy. No DAG code to maintain.

**They solve different problems.** Airflow is for scheduled batch
processing where you need cron-like triggers and the latency between
steps can be measured in seconds. Durga is for continuous event-driven
processing where data arrives unpredictably, every step must execute
immediately, and you want the pipeline topology to be visible in a
diagram rather than buried in code. Many organisations need both.

---

## The five questions you may ask

### 1. "Why not just use Airflow? It works."

Airflow works until:

- Your DAG has 50+ tasks and the codebase is thousands of lines of
  operator wiring.
- You need sub-second propagation between steps and the scheduler poll
  interval becomes your floor.
- A business analyst asks to see the pipeline and you have to fire up
  the Airflow UI, which renders a graph _from_ code — a graph that may
  not match the specification document written six months ago.
- You need per-step horizontal scaling and discover that Airflow scales
  workers, not individual tasks.
- You need an audit trail of every state transition and realise XComs
  and the metadata database don't give you an immutable event log.

In Durga, the BPMN diagram _is_ the specification, documentation, and
implementation blueprint. There is one artefact. When someone asks "what
does our pipeline look like?", you open the diagram. When the pipeline
needs to change, you edit the diagram and regenerate. The generated code
is standard Quarkus + Kafka — you can inspect it, version it, and
customise it if needed, but for the 15 plugin-driven operations you
never touch it. For operations that no plugin covers, custom activities
generate a contract interface — you implement it in a normal Java file,
and a build-time utility writes the implementation metadata back into the
BPMN model so regeneration preserves the connection.

### 2. "We already have Airflow running. What's the migration cost?"

You don't migrate. You add.

Durga pipelines emit to and consume from Kafka topics. An existing
Airflow DAG can publish to a Kafka topic that triggers a Durga pipeline.
A Durga pipeline can write to a topic consumed by downstream systems
your Airflow DAGs already talk to.

The practical path:

1. Pick one pipeline that is causing pain in Airflow — too slow, too
   much bespoke code, too hard to monitor.
2. Remodel it as a BPMN diagram. A pipeline with 5-10 steps takes an
   afternoon.
3. Generate it with Durga and deploy alongside the existing Airflow DAG.
4. Run both in parallel, compare throughput, latency, and operational
   load.
5. Cut over when confident. Retire the Airflow DAG.

The 15 built-in plugins mean most transformation logic doesn't need new
code. If you _do_ need custom logic, you implement the `Plugin` interface
(one method: `byte[] execute(byte[] payload, String config)`) and
register it. You never touch the generated worker code.

### 3. "Kafka is complex. Who's going to operate this?"

If your organisation runs Airflow, you already operate:
- A scheduler service
- A metadata database (Postgres)
- A message broker (Redis or RabbitMQ, depending on executor)
- Worker nodes
- The DAG codebase

Kafka replaces the broker with something more capable, and removes the
scheduler entirely. The operational surface area is comparable.

The generated services are standard Quarkus microservices. They use
`@Incoming` and `@Outgoing` annotations for Kafka channels. Any team
that runs Java services in production can operate them. The Kafka
Streams monitoring topology runs as a separate process and requires no
configuration per pipeline.

Yes, Kafka has a learning curve. But:
- Managed Kafka (Confluent Cloud, Aiven, Redpanda) eliminates broker
  operations.
- The generated code abstracts Kafka producer/consumer details behind
  Quarkus reactive messaging.
- Pipeline authors never write Kafka client code — they draw diagrams
  and configure plugins.
- The platform team operates Kafka once, for all pipelines.

### 4. "What happens when the generated code isn't enough?"

Four escape hatches, in order of preference:

**Use a plugin.** If you need a transform, filter, enrichment, or
validation step, implement the `Plugin` interface — a single method:
`byte[] execute(byte[] payload, String config)`. The 15 built-in
plugins show the pattern. Your plugin appears in the registry and can
be referenced from any BPMN diagram by name. No generated code changes.

**Implement a custom activity contract.** Mark the BPMN activity with
`camunda:plugin="custom"`. Durga generates a contract interface and a
delegating worker with CDI injection. You write the implementation in a
normal Java file — IDE support, compilation, unit tests. A build-time
utility (`ModelEnricher`) detects your implementation via reflection,
hashes the source file, and writes `customImpl`, `customSource`, and
`customHash` properties back into the BPMN model. On regeneration, the
scaffolder sees the enriched model and preserves the connection — it
regenerates the contract and worker but never touches your implementation
class. This is the recommended path for any logic that doesn't fit a
pre-built plugin.

**Modify the generated worker directly.** The generated code is clean
Java. You can add custom logic. On regeneration, existing files under
`src/main/java` are skipped — your modifications survive. This works
but is less clean than the contract approach because your modifications
live alongside generated boilerplate.

**For gateways, edit the BPMN model, not the code.** Condition
expressions on sequence flows (`${amount > 1000}`, `${status == "OK"}`)
are evaluated at runtime by a recursive descent parser in the generated
gateway classes. No code modification needed — change the expression in
Camunda Modeler and regenerate.

### 5. "Who's using this? Is it proven?"

Durga is at v0.1.0. It compiles 24 BPMN test fixtures covering the
full BPMN execution subset (tasks, gateways, timers, boundary events,
subprocesses, call activities, message/signal events, and data pipeline
plugins). 196 tests pass, including a compilation integration test that
verifies generated projects actually compile against their dependencies.
The generated projects run on Quarkus + Kafka.

It is not a 10-year-old Apache project. The counterargument: neither was
Airflow in 2016. The architecture — compile a model into a runtime — is
well-established (Terraform, Pulumi, XState all do this in other
domains). The runtime is Kafka, which is battle-tested.

The right question is not "is Durga mature?" but "does the BPMN-to-Kafka
mapping produce correct, observable, scalable pipelines for our use
case?" That can be answered with a proof of concept on one pipeline in
one sprint.

---

## The architectural bet you're making

| | Airflow | Durga |
|---|---|---|
| **Execution** | Central scheduler dispatches tasks | No scheduler. Each step is an always-on Kafka consumer. |
| **Latency floor** | Scheduler poll interval (typically 1-30s) | Kafka produce/consume (sub-millisecond) |
| **Throughput ceiling** | Scheduler database write throughput | Kafka partition count × consumer parallelism |
| **Scaling granularity** | Worker pool | Per-step consumer group |
| **Definition** | Python code in a DAG file | BPMN diagram + plugin configuration |
| **Change workflow** | Edit Python → PR → review → merge → deploy | Edit diagram → regenerate → deploy |
| **Line-of-business visibility** | Need Airflow UI or separate docs | Open the BPMN file in Camunda Modeler |
| **Monitoring** | Airflow UI + external tools | Built-in: per-pipeline event topic with Kafka Streams monitoring topology |
| **Error handling** | `on_failure_callback`, retry parameters | Structural: boundary events, DLQ per step, scope cancellation |
| **Audit** | Metadata database (mutable) | Immutable per-pipeline event topic (`process-events-{id}`) |
| **Custom code** | Write a Python operator | Implement a generated contract interface in Java; build-time enrichment writes metadata back into the BPMN model for regeneration safety |
| **Multi-pipeline isolation** | Shared scheduler database | Each pipeline gets its own event topic by default (`process-events-{id}`); override with `--event-topic` to share |
| **Pre-built operations** | Provider packages (HTTP, S3, etc.) | 15 governed plugins in a versioned YAML registry, referenced by name from BPMN |

The bet is this: **for event-driven pipelines, compiling a model into a
distributed runtime produces simpler, more observable, more scalable
systems than scheduling Python code.** The model is smaller than the
code it replaces. The runtime is decentralised. The monitoring is
automatic. The plugin catalog grows without touching generated code.
Custom logic lives in normal Java files with full IDE support, and the
model learns from the code at build time — not the other way around.

If your pipelines are cron-triggered batch jobs that process yesterday's
data, Airflow is the right tool. If they process events as they arrive
and you want the pipeline topology to be a first-class artefact, Durga
is a serious alternative.
