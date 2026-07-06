# Durga Data Pipeline Suitability Analysis

Date: 2026-07-02

## Executive Summary

Durga is a promising fit for **event-driven, Kafka-native data pipeline
management** where the desired architecture is a distributed set of always-on
processing stages, with BPMN as the durable process specification and Kafka as
the execution substrate.

It is **not yet a general replacement for mature data orchestrators** such as
Apache Airflow, Dagster, Prefect, or Apache Beam. The codebase has real pipeline
building blocks: BPMN-to-Kafka scaffolding, plugin-dispatched service tasks,
Kafka Connect skeleton generation, data asset metadata extraction, monitoring,
DLQ-oriented plugin error records, object-store payload reference plugins,
format detection, and a `DataHandle` runtime contract. The main gaps are
maturity, end-to-end asset/lineage execution semantics, schema governance,
backfill/replay tooling, production security manifests, and a larger connector
and plugin ecosystem.

The sibling project [`vannak`](https://github.com/FrodeRanders/vannak) improves
the architecture by providing an operational knowledge plane that consumes
Durga-compatible process events and adds data-individual provenance, passive/active
metadata, metadata references, durable outbox handling, hot indexes, and Ipto
placement. That means the combined **Durga + Vannak** is much stronger than Durga
alone for pipeline observability and metadata handling. The verdict changes from
"promising but missing a metadata plane" to "promising with a credible companion
metadata/provenance plane, but still integration- and maturity-limited."

The best near-term positioning is:

- Use Durga for a **controlled pilot** of one or two Kafka-centric pipelines.
- Include Vannak in that pilot if item-level metadata, provenance, or metadata
  impact analysis is part of the goal.
- Treat it as a **pipeline runtime/compiler experiment**, not as production
  platform software yet.
- Invest next in **first-class lineage, metadata, schema validation, replay, and
  production operations contracts**.

## Evidence From The Codebase

### 1. BPMN Models Compile To Kafka-Oriented Runtimes

Durga's main capability is in the scaffolder:

- `durga-tools/src/main/java/org/gautelis/durga/tools/BpmnScaffolder.java`
- `durga-tools/src/main/java/org/gautelis/durga/tools/BpmnModelCollector.java`
- `durga-tools/src/main/resources/templates-java/scaffold.stg`
- `durga-tools/src/main/resources/templates-java/scaffold-generated-project.stg`

The README describes the tool as "BPMN -> Kafka code generation and process
monitoring" and documents generated worker classes, task input/output topics,
helper scripts, generated `summary.json`, generated `application.yml`, and topic
scripts.

The generated model is naturally pipeline-like:

- each BPMN task maps to Kafka input/output topics,
- gateways route events between topics,
- process lifecycle events are emitted to a canonical event stream,
- generated Quarkus/SmallRye Reactive Messaging workers consume and produce
  Kafka messages.

This is a good architectural match for event-driven pipelines because the
runtime is decentralized: pipeline stages are independent Kafka consumers rather
than tasks dispatched by a central scheduler.

### 2. Pipeline Plugins Are Already Implemented

Pipeline functionality is provided by:

- `durga-runtime/src/main/java/org/gautelis/durga/plugins/Plugin.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/PipelinePlugin.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/JsonTransform.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/FieldFilter.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/JsonSchemaValidator.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/Mask.java`
- `durga-runtime/src/main/java/org/gautelis/durga/plugins/WindowCounter.java`
- `plugins/catalog.yml`

The `Plugin` contract supports text/JSON and binary payloads. The scaffolder
resolves plugin descriptors through `PluginRegistry` and generates plugin
executor classes for BPMN service tasks annotated with `plugin` and
`pluginConfig`.

Useful pipeline operations are present:

- JSON transformation and flattening,
- field filtering,
- masking,
- regex extraction,
- timestamp normalization,
- type coercion,
- UUID injection,
- schema validation,
- key/value enrichment,
- field-value routing (the `field-router` plugin, class `DeadLetterRouter`),
- window counting.

This is a strong foundation for a governed data-pipeline component catalog.

### 3. Data Assets Are Recognized As Metadata

Durga has an explicit model for pipeline data assets:

- `durga-runtime/src/main/java/org/gautelis/durga/DataHandle.java`
- `doc/data-pipeline-assets.md`
- `BpmnModelCollector.collectDataObjectSpecs`
- `BpmnModelCollector.collectDataStoreSpecs`
- `BpmnModelCollector.collectDataAssociationSpecs`
- `DataObjectSpec`, `DataStoreSpec`, and `DataAssociationSpec` in
  `BpmnModelSupport.java`

The principle: Kafka should carry process/control events and
small metadata, while large datasets should travel by reference through
`DataHandle`.

This matters for real data pipelines. Large Parquet files, object-store
partitions, table snapshots, and graph deltas should not be embedded in Kafka
process events. Durga already distinguishes:

- process control flow,
- logical data objects,
- physical data stores,
- data associations between tasks and assets,
- optional Kafka Connect skeleton generation for stores.

### 4. Monitoring Is Built Around Kafka Streams

Durga includes monitoring services under:

- `durga-monitor/src/main/java/org/gautelis/durga/monitoring/` (Kafka Streams topology, REST API, CLI) and `durga-runtime/src/main/java/org/gautelis/durga/monitoring/Metrics.java` (component metrics)
- `monitoring-ui/`

The monitoring topology materializes:

- latest process state,
- counts by state,
- active instance index,
- activity latency summaries,
- lifecycle trend buckets,
- BPMN model cache.

The monitoring API exposes process counts, latency, stuck instances, diagrams,
health, and Prometheus-style metrics. This is relevant to pipelines because
pipeline operators need step-level state, latency, stuck-run detection, and
visual topology.

The gap is that these are currently process metrics, not rich data-pipeline
metrics such as freshness, row counts, schema versions, partition completeness,
quality score, lineage graph, or data contract status.

### 5. Kafka Connect Skeletons Exist, But Are Not Complete Connectors

`--connect` generation is implemented in `BpmnScaffolder` and
`scaffold-connect.stg`.

Durga can generate:

- generic source/sink connector JSON,
- per-data-store connector skeletons,
- connector class hints for S3 sink, JDBC source/sink, and Neo4j sink,
- `connect.sh` to post connector configs.

This is useful because data pipelines usually need external system edges. But
the generated configs are intentionally incomplete: credentials, converters,
connector-specific settings, transforms, tables, buckets, and operational error
handling still need to be filled in.

### 6. Payload Externalization And Format Detection Are Emerging

Durga now includes three additional pipeline plugins:

- `format-detector` detects coarse format, datatype, media type, encoding, byte
  count, and SHA-256 hash.
- `object-store-collector` writes the current payload to a local file-backed
  object store and emits a `DataHandle`-style reference.
- `object-store-extractor` resolves that handle and emits the referenced bytes
  to the next topic.

These plugins are important because they move Durga closer to the intended
data-plane design in `doc/data-pipeline-assets.md`: Kafka carries orchestration
events and references, while large payloads can live outside Kafka. The current
implementation is local-file backed, so cloud/object-store adapters such as S3,
GCS, Azure Blob, or MinIO remain future work.

## Effect Of Vannak

The sibling project `vannak` ([https://github.com/FrodeRanders/vannak](https://github.com/FrodeRanders/vannak))
implements a separate operational knowledge plane around Durga-style process
events and data-individual metadata.

### What Vannak Adds

Evidence from [`vannak`](https://github.com/FrodeRanders/vannak):

- `src/durga.rs` mirrors Durga's canonical `ProcessEvent` shape and converts it
  into Vannak `PipelineEvent` values with tenant, environment, source sequence,
  metadata references, and schema-version context.
- `src/data.rs` defines `DataIndividualMetadataEvent`, `DataIndividualId`,
  `DataIndividualShardId`, passive metadata, active metadata, plugin identity,
  payload references, operations such as received/transformed/masked/validated/
  enriched/routed/persisted, and idempotency keys.
- `src/metadata.rs` defines typed metadata references for datasets, schemas,
  fields, pipeline definitions, metadata objects, lineage edges, data contracts,
  owners, and classifications.
- `src/data.rs` also implements `DataProvenanceIndex`, with lookup by data
  individual, process instance, and activity.
- `src/index.rs` implements a hot process index over pipeline events, current
  process state, status queries, time-range queries, and metadata-impact lookup.
- `src/service.rs` wires process ingest, data-individual metadata capture,
  placement resolution, durable metadata outbox enqueue, Ipto
  ([https://github.com/FrodeRanders/ipto](https://github.com/FrodeRanders/ipto))
  write draining, snapshots, and provenance queries.
- `src/kafka_ingest.rs` defines the Kafka/Sitas admission boundary: Kafka
  topic/partition/offset identity is retained, optional process-event journaling
  happens before Sitas ([https://github.com/FrodeRanders/sitas](https://github.com/FrodeRanders/sitas))
  admission, and offsets become committable only after the durable/runtime boundary
  is crossed.
- `tests/vannak_integration.rs` includes a gated full ingest -> index -> outbox
  -> Ipto writer flow.
- `tests/kafka_integration.rs` includes a gated Kafka Durga-JSON -> Sitas worker
  smoke path.

### How This Changes The Verdict

Durga alone is best described as a Kafka-backed BPMN runtime/scaffolder with
process monitoring and early data-pipeline primitives. Vannak provides the
missing shape of a metadata/provenance plane.

The combined architecture is stronger in these areas:

- **Item-level provenance:** Vannak models a flowing data item explicitly
  through `DataIndividualId` and `DataIndividualMetadataEvent`.
- **Passive metadata:** source, format, content type, encoding, size, checksum,
  tenant, environment, and payload references fit Vannak's passive metadata
  model.
- **Active metadata:** transformations, masking, validation, enrichment, field
  changes, and plugin identity fit Vannak's active metadata model.
- **Metadata impact analysis:** Vannak indexes events by typed metadata refs and
  can answer which process events or pipelines are associated with a dataset,
  schema, field, contract, owner, classification, or lineage edge.
- **Durable metadata capture:** Vannak has a segment-backed metadata outbox and
  idempotent Ipto write payload construction.
- **Operational hot index:** Vannak can maintain recent process and provenance
  state independently of Durga's monitoring UI.
- **Placement and scaling direction:** Vannak separates runtime shard placement
  from durable metadata ownership via `DataIndividualShardId` and Ipto placement maps.

In consequence:

> Durga plus Vannak is suitable for a more serious Kafka-native pipeline pilot
> where item-level metadata, provenance, and metadata-impact questions are part
> of the goal. It is still not a turnkey production replacement for mature
> orchestrators, because the generated Durga event emission now exists but the
> cross-project schema, Vannak-side ingestion path, and production operations
> contract still need to be hardened.

### What Still Remains Open With Vannak Present

Vannak reduces but does not eliminate the key gaps:

- Durga generated plugin and custom workers now emit Vannak-compatible
  `DataIndividualMetadataEvent` records automatically to `vannak-metadata-events`.
  This is a first bridge, not yet a production interoperability package.
- Durga's `ProcessEvent` Java record does not yet carry typed metadata refs,
  schema version, item identity, source offsets, or lineage fields in a way that
  maps directly to Vannak without adapter conventions.
- The object-store collector, extractor, format detector, and handle-aware
  execution path produce useful metadata, but their mapping into Vannak passive
  and active metadata should be stabilized as a schema contract.
- Vannak's Kafka ingest path must consume and index the Durga
  `vannak-metadata-events` topic as a supported compatibility path.
- Vannak's full Ipto integration and Kafka smoke tests are gated integration
  tests, not always-on release gates.
- There is no shared schema/contract package between Durga Java and Vannak Rust
  for process events, metadata events, and data handles.
- Vannak appears intentionally pre-product: strong core types and service
  boundaries exist, but production deployment, background writer tasks,
  checkpoint publication, automatic segment discovery, and operator APIs remain
  maturity work.

## Comparison With Other Solutions

### Apache Airflow

Airflow is a mature workflow platform where workflows are DAGs of tasks. The
official Airflow architecture documentation describes a scheduler, DAG
processor, webserver, DAG files, metadata database, workers, triggerers, and
plugins. It also states that Airflow is agnostic to what tasks run and can
orchestrate anything through providers, shell commands, or Python operators.

Airflow strengths:

- mature scheduler and operational model,
- broad provider ecosystem,
- strong batch scheduling, retry, backfill, and UI features,
- familiar Python authoring model,
- managed offerings are widely available.

Airflow weaknesses for Durga's target:

- central scheduler and metadata database remain core control-plane components,
- pipeline definition is code-first rather than business-model-first,
- event-driven behavior exists but is not the core execution substrate,
- Kafka is usually an integration point, not the runtime backbone,
- small metadata transfer via XCom is not a full lineage or data-plane model.

Durga is more compelling than Airflow when the pipeline is naturally Kafka-first,
long-running, event-driven, low-latency, and each stage should scale as an
independent consumer group. Airflow remains better for conventional scheduled
batch workflows, broad third-party integration, mature backfills, and production
orchestration today.

Source: Apache Airflow 3.2.2 architecture docs:
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html

### Dagster

Dagster is strongly asset-oriented. Its docs define assets as persistent
objects such as tables, files, or models, and asset definitions describe what
assets should exist and how they are produced. Dagster also has a mature concept
set around asset dependencies, metadata, checks, partitions, freshness,
backfills, automation, and observability.

Dagster strengths:

- first-class data asset model,
- lineage and asset graph are central concepts,
- strong data quality and asset check story,
- good fit for analytics engineering and modern data platform workflows,
- mature developer ergonomics for Python data stacks.

Durga compared with Dagster:

- Durga's `DataHandle`, data object parsing, and data store parsing point in a
  Dagster-like direction, but they are not yet first-class runtime semantics.
- Durga's Kafka-native execution can be better for continuous event pipelines.
- Dagster is currently much stronger for asset lineage, partitions, checks,
  freshness, and operational maturity.

Durga would need substantial asset governance work before it can compete with
Dagster as an asset-centric data orchestration platform.

Source: Dagster asset docs:
https://docs.dagster.io/guides/build/assets

### Prefect

Prefect models flows as decorated Python functions. Its docs emphasize flow and
task state tracking, retries, timeouts, deployments, schedules, automations,
events, typed parameters, and observability through the Prefect backend.

Prefect strengths:

- lightweight Python workflow authoring,
- strong runtime state tracking,
- flexible deployment model,
- good developer experience for Python teams,
- easy orchestration of arbitrary Python code.

Durga compared with Prefect:

- Durga is more model-driven and Kafka-native.
- Prefect is more mature and easier for Python-centric teams.
- Prefect tracks workflow state but does not make Kafka topics the native
  execution boundary.
- Durga's BPMN and generated Java services may be more explicit for long-lived
  operational pipelines, but at the cost of ecosystem maturity.

Source: Prefect flow docs:
https://docs.prefect.io/v3/concepts/flows

### Apache Beam

Beam is a data processing programming model, not primarily an orchestrator. The
official programming guide covers PCollections, transforms, I/O connectors,
schemas, windowing, triggers, metrics, state, timers, and runners.

Beam strengths:

- mature batch and streaming data processing semantics,
- windowing, triggers, watermarks, state, timers,
- runner portability across systems such as Flink, Spark, and Dataflow,
- strong fit for high-volume data transformation.

Durga compared with Beam:

- Durga is about process orchestration on Kafka, not replacing a distributed
  dataflow engine.
- Beam is better for complex event-time processing, joins, watermarks, and
  high-volume transformations.
- Durga could orchestrate Beam jobs or wrap Beam/Flink stages as tasks, but
  should not reimplement Beam's data processing semantics.

Source: Apache Beam programming guide:
https://beam.apache.org/documentation/programming-guide/

## Suitability Verdict

Durga is suitable when most of these are true:

- Kafka is already the strategic platform.
- Pipelines are event-driven or near-real-time.
- BPMN diagrams are valuable as shared specifications.
- The pipeline can be decomposed into independent stages.
- Process state, routing, waiting states, timers, boundary events, and
  human/manual steps matter.
- Java/Quarkus services are acceptable operational artifacts.
- The team can tolerate prototype/beta maturity while investing in missing
  platform capabilities.

Durga is not yet suitable when most of these are true:

- You need mature production orchestration immediately.
- You depend heavily on existing Airflow/Dagster/Prefect provider ecosystems.
- You need built-in data asset lineage, freshness, partition backfills, and data
  quality checks today.
- Pipelines are primarily scheduled batch jobs.
- You need complex stream processing semantics such as event-time joins,
  watermarks, late-data handling, or large aggregations.
- Python is the required implementation language for all pipeline code.
- You need mature RBAC, multi-tenancy, managed deployment, audit UI, and
  production support out of the box.

Short answer: **Durga is a credible architecture for Kafka-native pipeline
management. With Vannak beside it, the metadata and provenance story becomes
credible rather than merely planned. The combined system still needs integration
contracts and production hardening before it should be treated as a primary
production data platform.**

## Gaps That Would Hinder Data Pipeline Use

### 1. Lineage Is Planned, Not First-Class

`doc/data-pipeline-assets.md` describes future lineage fields, but
`ProcessEvent` does not yet have explicit lineage fields such as reads, writes,
schema version, checksums, row counts, source offsets, or artifact identifiers.

Vannak changes this gap from "missing model" to "integration and hardening."
Vannak has data-individual provenance events, passive/active metadata, source
payload refs, idempotency keys, and Ipto outbox handling. Durga now emits a
Vannak-compatible companion event from generated plugin and custom workers, but
the event schema, Vannak consumer, and release checks still need to be treated as
formal cross-project contracts.

Impact:

- monitoring cannot answer "which task produced this dataset?",
- audit is process-state-oriented rather than data-asset-oriented,
- replay and debugging require payload inspection or external convention.

### 2. Data Asset Runtime Semantics Are Thin

The scaffolder collects BPMN data objects, stores, and associations, and
`DataHandle` exists. Generated workers can now pass handles manually or
materialize them through `PluginExecutionSupport`, and the object-store collector
and extractor provide a concrete local-file-backed reference flow. But generated
workers do not yet enforce BPMN read/write contracts, validate schemas, or create
typed asset interfaces.

Impact:

- BPMN data assets are useful documentation and generated metadata,
- they do not yet govern execution.

Vannak can store and index item-level metadata once emitted, but Durga still
needs generated runtime checks that bind BPMN data associations to actual
payload handles and metadata events.

### 3. Schema Governance Is Minimal

`JsonSchemaValidator` exists as a plugin, but there is no platform-wide schema
registry integration, compatibility policy, generated typed payload contracts,
or schema-version propagation in lifecycle events.

Impact:

- pipeline contracts are convention-based,
- breaking payload changes are not caught consistently at scaffold or runtime.

Vannak's `MetadataRef::Schema` and `schema_version` support give a place to put
schema meaning, but Durga still needs schema registry integration and generated
compatibility checks.

### 4. Backfill And Replay Need Product Semantics

Kafka makes replay possible, but Durga needs explicit tooling and contracts for:

- replaying from offsets,
- re-running one pipeline instance,
- re-running one failed stage,
- backfilling a partition/date range,
- preventing duplicate writes,
- recording replay lineage separately from original lineage.

Impact:

- operational recovery remains manual and risky.

### 5. Idempotency Is A Plugin Contract, But Side-Effect Protection Is Partial

The `Plugin` contract now includes a structured result path: plugins may override
`executeWithResult` to return a `PluginResult` carrying an idempotency key (default:
content hash of payload + config), an `OutputDisposition`
(`PAYLOAD`/`PASSTHROUGH`/`SIDE_EFFECT`), and an error strategy
(`DLQ`/`SKIP`/`FAIL`). What remains missing is an end-to-end external-write
protocol (e.g. transactional outbox / exactly-once sink) that consumes the
idempotency key.

Impact:

- retries and replays carry an idempotency key, but preventing duplicate
  *external* writes still depends on the sink honouring it.

### 6. Kafka Connect Generation Is Skeleton-Level

Generated connector configs provide topology-aware starting points, not
production connectors.

Impact:

- useful for onboarding,
- not enough for governed deployment without additional operator manifests,
  secrets, converters, transforms, ACLs, and validation.

### 7. Production Hardening Is Documented But Incomplete

`doc/operations-hardening.md` explicitly lists open work around topic
manifests, ACL examples, backup/replay procedures, and versioned compatibility
guarantees.

Impact:

- the project is honest about production gaps,
- these gaps matter for regulated or critical data pipelines.

### 8. Advanced Data Processing Should Be Delegated

Durga has plugins and a small experimental window counter, but it should not
try to become Beam, Flink, or Spark.

Impact:

- complex joins, watermarks, late data, large aggregations, and distributed
  compute should be delegated to specialized engines,
- Durga should orchestrate those engines and capture metadata/lineage.

## Recommended Next Steps

### Phase 1: Make Pipeline Metadata First-Class

Add explicit pipeline metadata to the runtime event contract:

- `runId`,
- `datasetId`,
- `assetHandles`,
- `schemaRef`,
- `schemaVersion`,
- `sourceOffsets`,
- `rowCount`,
- `byteCount`,
- `checksum`,
- `producerPlugin`,
- `producerPluginVersion`.

Implementation direction:

- keep `ProcessEvent` focused on lifecycle/control facts,
- continue emitting companion `DataIndividualMetadataEvent` records for item-level
  data facts,
- add a `LineageEvent` or `DataAssetEvent` record only if non-Vannak deployments
  need a Durga-native metadata stream,
- update monitoring topology to materialize asset views.

With Vannak present, prefer a separate `DataIndividualMetadataEvent` emission
path over overloading `ProcessEvent`. Durga now follows that direction for
generated plugin and custom workers.

### Phase 2: Enforce Data Associations In Generated Code

Use collected `DataAssociationSpec` metadata to generate contracts:

- task input asset declarations,
- task output asset declarations,
- required `DataHandle` payload keys,
- generated README sections showing task read/write assets,
- validation warnings when BPMN associations are incomplete.

This turns BPMN data objects from documentation into executable pipeline
contracts.

The generated code should use BPMN data associations to decide when to emit
Vannak metadata events and which `DataIndividualId`, data asset, schema, and
payload reference each task expects or produces.

### Phase 3: Add Schema Registry And Compatibility Checks

Introduce schema governance at scaffold and runtime:

- support schema refs on BPMN data objects,
- validate plugin output against declared output schemas,
- integrate with Confluent Schema Registry or Apicurio Registry,
- generate typed Java records where practical,
- fail scaffolding or generation when required schema metadata is missing.

### Phase 4: Build Replay And Backfill Tooling

Add operator-facing commands:

- replay process instance,
- replay task from DLQ,
- replay topic partition/offset range,
- backfill by business key/date partition,
- dry-run replay plan,
- record replay reason and source event IDs.

The important design point is to make replay observable and idempotent, not just
possible through Kafka.

### Phase 5: Strengthen Plugin SDK Contracts

Evolve the plugin contract beyond `execute(bytes, config)`. Several of these are
already implemented via `executeWithResult`/`PluginResult` (marked *done*):

- plugin descriptor config-schema validation,
- plugin version resolution,
- declared input/output media types,
- idempotency key strategy — *done* (`Plugin.idempotencyKey`, default content hash),
- side-effect declaration — *done* (`OutputDisposition.SIDE_EFFECT` + description),
- lineage emission helper — *done* (Vannak `DataIndividualMetadataEvent` emission),
- structured plugin result with payload plus metadata — *done* (`PluginResult` with metadata),
- standard error strategies: fail, skip, DLQ — *done* (`ErrorStrategy`); `quarantine` remains future work.

This should align with Vannak's passive/active metadata distinction. Plugin
execution should return payload bytes plus metadata facts such as transformed
fields, validation result, masking fields, enrichment source, before/after
checksums, and source payload refs.

### Phase 5b: Harden The Durga-Vannak Bridge

Turn the current generated emission path into a concrete interoperability
contract:

- Keep the Java record and Rust type for `DataIndividualMetadataEvent` aligned.
- Shared JSON schema or Avro/Protobuf schema for the metadata event.
- Generated Durga emitters for Vannak metadata topics, currently the shared
  `vannak-metadata-events` topic.
- Mapping from `DataHandle` and plugin metadata into Vannak passive/active
  metadata fields.
- Stable idempotency-key rules.
- Adapter tests that take generated Durga metadata events and verify Vannak
  ingestion/indexing.

### Phase 6: Production Operations Package

Generate or document:

- Strimzi topic manifests with retention and compaction policies,
- Kafka ACL examples by generated worker role,
- KEDA scaling manifests tied to lag and latency,
- Prometheus alert rules,
- DLQ replay runbooks,
- state-store recovery runbooks,
- upgrade compatibility tests for event contracts.

### Phase 7: Position Durga With, Not Against, Existing Engines

Durga should integrate with existing data processing systems:

- Airflow/Prefect can trigger or consume Durga pipeline edges during migration.
- Beam/Flink/Spark jobs can be represented as BPMN service tasks or custom
  activities.
- Dagster-like asset semantics can inspire Durga's asset metadata and lineage
  model without copying its execution model.

## Recommended Pilot

Choose a pipeline with these properties:

- Kafka input or output already exists,
- 5-10 processing steps,
- includes at least one validation or quarantine branch,
- includes one external sink,
- has clear audit and lineage requirements,
- does not require complex event-time joins or large distributed computation.

Pilot success criteria:

- scaffolded project compiles without manual worker edits,
- plugin tasks cover most transformation/validation steps,
- generated connector skeletons are usable after environment-specific filling,
- monitoring shows live state, latency, and stuck instances,
- generated metadata can identify produced and consumed assets,
- a failed record can be DLQ'd, inspected, corrected, and replayed,
- a documented lineage record exists for every output artifact.
- Vannak can query metadata by data individual, process instance, and activity.
- Vannak can answer which metadata objects or datasets are affected by a failed
  process activity.

## Conclusion

Durga's design is aligned with a specific and valuable niche: **BPMN-modeled,
Kafka-native, event-driven pipeline orchestration with generated Java services
and built-in process monitoring**.

Vannak strengthens that niche by adding the missing operational knowledge plane:
data-individual provenance, metadata references, durable metadata outbox,
metadata-impact indexing, and Ipto placement. That does not make the combined
system production-ready, but it makes the architecture more coherent and much
more competitive for governed data pipelines than Durga alone.

The codebase already contains enough pipeline-specific functionality to justify
a serious proof of concept. It should not yet be considered production-ready
against Airflow, Dagster, Prefect, or Beam in their areas of strength. The next
engineering work should make data assets, lineage, schema governance, replay,
idempotency, and production operations first-class rather than conventions.

If those gaps are addressed, Durga could become a strong option for organizations
that want data pipelines to be explicit process models running directly on
Kafka, rather than scheduler-managed code graphs.
