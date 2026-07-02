# BPMN Data Assets in Durga

Durga should support BPMN data objects as pipeline data assets, not as Kafka topics.

The runtime distinction is:

- Sequence flows control execution.
- Kafka topics carry process events, worker inputs, worker outputs, messages, and signals.
- BPMN data objects describe logical datasets or artifacts.
- BPMN data stores describe physical sources and sinks such as S3, PostgreSQL, Neo4j, local files, object stores, or APIs.
- Tasks, plugins, and connectors read and write data assets.

This keeps Kafka as the orchestration substrate while allowing the actual data plane to use the storage systems appropriate for large datasets.

## Model

A BPMN data object represents a named business artifact:

```text
RawOrders
NormalizedOrders
CustomerGraphDelta
InvoiceBatch
```

A BPMN data store represents a physical location or service:

```text
S3Warehouse
PostgresOrders
Neo4jCustomerGraph
LandingBucket
```

Data input and output associations connect tasks to those assets:

```text
Read Orders
  output: RawOrders

Normalize Orders
  input: RawOrders
  output: NormalizedOrders

Build Customer Graph
  input: NormalizedOrders
  output: CustomerGraphDelta

Write S3
  input: NormalizedOrders
  target: S3Warehouse

Write Neo4j
  input: CustomerGraphDelta
  target: Neo4jCustomerGraph
```

The data object is not a transport channel. It is metadata used to generate contracts, schema stubs, data handles, lineage, and connector/plugin configuration.

## Runtime Representation

Large pipeline payloads should travel by reference. A `ProcessEvent.payload()` can carry a `DataHandle` rather than the dataset itself:

```json
{
  "normalizedOrders": {
    "name": "NormalizedOrders",
    "uri": "s3://warehouse/orders/normalized/run-42/",
    "mediaType": "application/parquet",
    "schema": "schemas/normalized-orders.avsc",
    "metadata": {
      "rowCount": 120034,
      "checksum": "sha256:..."
    }
  }
}
```

This keeps process events small and makes Durga useful for pipeline management:
Kafka carries lifecycle/control events and small metadata, while large datasets
travel by reference through `DataHandle`.

Durga includes experimental support for this pattern:

- `object-store-collector` writes the current payload to a local file-backed object store and forwards a `DataHandle`-style JSON reference.
- `object-store-extractor` resolves a `DataHandle`-style JSON reference and emits the referenced bytes.
- Generated plugin and custom workers can use `handleMode=manual` to operate on the handle JSON itself, or `handleMode=materialize` to load referenced bytes, run the plugin, write the output back to object storage, and forward a new handle.
- Successful plugin and custom worker executions emit a Vannak-compatible `DataIndividualMetadataEvent` to `vannak-metadata-events`, including process/activity context, payload size/checksum facts, plugin metadata, and handle/format metadata when present.

## BPMN Extension Properties

Durga can enrich BPMN data elements with Camunda properties:

```xml
<camunda:property name="schema" value="schemas/normalized-orders.schema.json" />
<camunda:property name="mediaType" value="application/json" />
<camunda:property name="kind" value="s3" />
<camunda:property name="uri" value="s3://warehouse/orders/normalized/" />
```

For data stores, `kind` identifies the adapter family, for example `s3`, `postgresql`, `neo4j`, `file`, or `http`. If `kind` is omitted, Durga can infer it from the URI scheme when possible.

## Scaffolder Behavior

The scaffolder should:

1. Parse BPMN `dataObject`, `dataObjectReference`, `dataStoreReference`, `dataInputAssociation`, and `dataOutputAssociation`.
2. Emit structured data metadata in `summary.json`.
3. Document assets, stores, and task read/write associations in the generated README.
4. Generate a runtime `DataHandle` contract.
5. Generate Kafka Connect skeletons from data stores when `--connect` is used.
6. Generate Vannak metadata event support for plugin and custom workers.
7. Later generate schema stubs and typed task contracts from data objects.
8. Later generate richer store/client adapters for cases that do not fit Kafka Connect or the local-file object-store plugins.

The first implementation slices are metadata/runtime contract support, optional
Kafka Connect skeletons for data-store edges, local-file object-store
collector/extractor plugins, handle-aware plugin execution, and Vannak companion
metadata events. They deliberately do not turn BPMN data objects into topics.

## Kafka Connect

Kafka Connect belongs at pipeline edges, not in the process model core.

When `--connect` is used, Durga can inspect data-store associations:

- A task input association whose source is a data store becomes a source connector skeleton that writes to the task input topic.
- A task output association whose target is a data store becomes a sink connector skeleton that reads from the task output topic.

For example:

```text
Enrich Data
  output: EnrichedCustomerData
  target stores: S3Warehouse, Neo4jCustomerGraph, PostgresCustomerStore
```

can generate connector skeletons such as:

```text
connect/data-stores/data_pipeline_demo-s3-warehouse-sink.json
connect/data-stores/data_pipeline_demo-neo4j-customer-graph-sink.json
connect/data-stores/data_pipeline_demo-postgresql-customer-store-sink.json
```

Those connector configs consume existing Durga task output topics, for example:

```text
data_pipeline_demo_enrich_data_output
```

The connector config still needs real deployment-specific settings: credentials, converter choices, connector plugin class availability, transforms, table/bucket/index details, and error handling. Durga generates the topology-aware skeleton and keeps the data object as metadata.

## Data Plane Direction

Durga currently has local-file-backed object-store plugins and generated
handle-aware execution support. The longer-term data plane should be
adapter-based:

```java
interface DataStoreClient {
    DataHandle write(String assetName, byte[] data, Map<String, Object> metadata);
    byte[] read(DataHandle handle);
}
```

Concrete adapters can then support S3, PostgreSQL, Neo4j, local files, and other systems. Plugins remain responsible for transformation and domain-specific I/O; BPMN data objects/stores give those plugins explicit contracts and generated configuration.

## Lineage

Durga now emits Vannak-compatible companion metadata events from generated plugin
and custom workers. The next step is to bind BPMN data associations more tightly
to those events, so task completions and metadata streams can include data
lineage such as:

```json
{
  "read": ["RawOrders"],
  "wrote": ["NormalizedOrders"],
  "stores": ["S3Warehouse"],
  "rowCount": 120034,
  "checksum": "sha256:..."
}
```

This lets monitoring answer pipeline questions without inspecting payload bytes:

- Which task produced this dataset?
- Which schema/version was used?
- Where is the physical artifact?
- Which downstream tasks consumed it?
- Which external systems were written?
