# Data Pipeline Blueprint

Durga maps BPMN models to Kafka topic topologies. For data pipelines this is
architecturally natural — every BPMN step is an independent processing stage,
and every gateway is a routing decision. This document describes how to evolve
the scaffolder from a generic BPMN-to-Kafka tool into a **plugin-driven data
pipeline generator** where BPMN task types directly correspond to reusable,
governed data processing components.

---

## 1. Architecture

```
BPMN model                    Plugin registry                 Generated runtime
┌──────────────┐              ┌──────────────────┐            ┌───────────────────┐
│ ServiceTask  │── pluginRef──│ plugin descriptor│── template──│ plugin executor   │
│   (transform)│              │  inputs/outputs  │            │  (topic consumer) │
│              │              │  config schema   │            │                   │
│ ExclusiveGw  │── condition──│ (routing logic)  │── template──│ router service    │
│  (route)     │              │                  │            │  (conditional)    │
└──────────────┘              └──────────────────┘            └───────────────────┘
```

A BPMN `ServiceTask` carries a `plugin` extension attribute pointing to a
registered plugin descriptor. The scaffolder resolves the descriptor, validates
the plugin configuration, and generates a concrete executor class instead of a
generic stub.

---

## 2. Plugin Registry

A plugin is defined by a YAML descriptor file that lives under
`plugins/` in the Durga repository or in a separate plugin catalog.

### 2.1 Descriptor format

```yaml
# plugins/transform/json-transform.yml
id: json-transform
name: JSON Transform
version: 1.0.0
category: transform
description: |
  Applies a JQ-like transformation to the input payload. The transform
  expression is evaluated against each incoming message.

input:
  schema:
    type: object
    description: JSON payload to transform

output:
  schema:
    type: object
    description: Transformed JSON payload

config:
  expression:
    type: string
    required: true
    description: JQ-style expression (e.g. '.data | {name: .name, total: .amount}')
    default: "."
  errorStrategy:
    type: enum
    values: [skip, dlq, fail]
    default: dlq
    description: How to handle transformation errors
```

### 2.2 Registry structure

```
plugins/
├── catalog.yml                    # master index of all plugins
├── transform/
│   ├── json-transform.yml
│   ├── field-filter.yml
│   └── type-cast.yml
├── validate/
│   ├── json-schema.yml
│   └── field-required.yml
├── enrich/
│   ├── kv-lookup.yml
│   └── http-enrich.yml
├── route/
│   ├── field-router.yml
│   └── weighted-split.yml
├── aggregate/
│   └── window-count.yml
└── sink/
    ├── kafka-sink.yml
    └── dead-letter.yml
```

The catalog is a flat index for tooling and validation:

```yaml
# plugins/catalog.yml
plugins:
  - id: json-transform
    version: 1.0.0
    path: transform/json-transform.yml
  - id: field-filter
    version: 1.0.0
    path: transform/field-filter.yml
  # ...
```

---

## 3. Component Type Governance

Plugin categories form a **governed taxonomy**. Each category defines a contract
that all plugins in that category must follow — same topic pattern, same lifecycle
events, same error channel conventions.

### 3.1 Category contracts

| Category | Topic pattern | Lifecycle events | Error channel | Notes |
|----------|--------------|-----------------|---------------|-------|
| **transform** | `%p%_%t%_input` → `%p%_%t%_output` | `ACTIVITY_ENTERED`, `ACTIVITY_COMPLETED`, `PROCESS_FAILED` | `%p%_%t%_dlq` | Always 1:1 input to output |
| **validate** | `%p%_%t%_input` → `%p%_%t%_output` + `%p%_%t%_invalid` | Same as transform + `ACTIVITY_ESCALATED` | `%p%_%t%_dlq` | Invalid branch + valid branch |
| **enrich** | `%p%_%t%_input` → `%p%_%t%_output` | Same as transform | `%p%_%t%_dlq` | May block on external lookup |
| **route** | `%p%_%t%_input` → N × downstream topic | `GATEWAY_TAKEN` | N/A | Replaces gateway templates |
| **aggregate** | `%p%_%t%_input` → `%p%_%t%_output` | `ACTIVITY_ENTERED` (window start), `ACTIVITY_COMPLETED` (window emit) | `%p%_%t%_dlq` | Windowing semantics |
| **sink** | `%p%_%t%_input` → (external) | `ACTIVITY_ENTERED`, `ACTIVITY_COMPLETED` | `%p%_%t%_dlq` | Terminal operations |

### 3.2 Adding a new category

1. Define the contract in this document (topics, lifecycle, error handling).
2. Create the category directory in `plugins/`.
3. Implement at least one reference plugin in the category.
4. Add the category contract to the scaffolder's validation logic.
5. Update the plugin template generator to handle the new category.

### 3.3 Plugin versioning

- **Semantic versioning**: `major.minor.patch`.
- Config schema changes that add optional fields → minor bump.
- Config schema changes that break existing configs → major bump.
- The scaffolder validates that the BPMN model's `pluginVersion` constraint matches.

### 3.4 Plugin lifecycle states

```
proposed → experimental → stable → deprecated → retired
```

State is declared in the descriptor and enforced by the scaffolder:
- `experimental`: generates with a warning
- `stable`: fully supported
- `deprecated`: generates with a deprecation notice
- `retired`: refuses to generate

---

## 4. BPMN Extension Binding

Plugins are bound to BPMN tasks via Camunda extension attributes on the
`ServiceTask` element.

### 4.1 Extension attributes

```xml
<bpmn:serviceTask id="transform_invoice"
                  name="Transform Invoice"
                  camunda:plugin="json-transform"
                  camunda:pluginVersion="^1.0"
                  camunda:pluginConfigExpression="'.'">
  <bpmn:extensionElements>
    <camunda:properties>
      <camunda:property name="plugin" value="json-transform" />
      <camunda:property name="pluginVersion" value="^1.0" />
      <camunda:property name="pluginConfig" value=".data | {name: .name, total: .amount}" />
    </camunda:properties>
  </bpmn:extensionElements>
</bpmn:serviceTask>
```

The scaffolder resolves these to plugin configuration:
- `plugin` → which descriptor to load
- `pluginVersion` → version constraint (semver range)
- `pluginConfig` → inline configuration string (or reference to a config file)

### 4.2 Alternative: explicit config reference

For complex configurations:

```xml
<camunda:property name="pluginConfigFile" value="pipelines/invoice/transform-config.yml" />
```

---

## 5. Template Architecture

The scaffolder currently generates generic stubs for tasks. With plugins, the
generation splits into two paths:

### 5.1 Template selection

```
Task detection
  ├── Is there a plugin attribute?
  │     ├── Yes → resolve plugin descriptor → generate pluginExecutorClass
  │     └── No  → fall back to existing task templates (worker, user, etc.)
```

### 5.2 Plugin executor template (`pluginExecutorClass`)

Generated executors use the `Plugin` interface. The template instantiates the
plugin class at field level and delegates every message to `plugin.execute()`:

```java
@ApplicationScoped
public class TransformDataPluginExecutor {
    private final Plugin plugin = new JsonTransform();

    @Incoming("%inputChannel%")
    public CompletionStage<Void> handle(Message<String> msg) {
        String output = plugin.execute(payload, "%config%");
        // emit ACTIVITY_COMPLETED, route output, handle DLQ
    }
}
```

The implementation class and config string are resolved from the plugin
descriptor at scaffold time. No per-plugin code paths are needed in the template.

---

## 6. Task Implementation Plan

The following plugins should be implemented as reference implementations
and test harnesses for the plugin architecture.

### 6.1 JSON Transform (category: transform)

Applies a template expression to transform one JSON structure into another.

**Config:**
- `expression` (string, required) — template expression
- `engine` (enum: `jq`, `jsonpath`, `handlebars`, default: `jq`)
- `errorStrategy` (enum, default: `dlq`)

**Implementation strategy:** Use a simple key-remapping approach with dot-notation
paths, avoiding external dependencies on JQ or Handlebars for now. The expression
format uses dot-notation for field access and simple template strings.

### 6.2 Field Filter (category: transform)

Keeps or drops specific fields from the input payload.

**Config:**
- `keep` (list of strings) — fields to retain (whitelist)
- `drop` (list of strings) — fields to remove (blacklist)
- If both are empty, passthrough; if both are set, `keep` wins.

### 6.3 JSON Schema Validator (category: validate)

Validates input payloads against a JSON Schema and routes valid/invalid to
separate output channels.

**Config:**
- `schema` (string, required) — inline JSON Schema or `schemaFile` path
- `onInvalid` (enum: `dlq`, `skip`, `fail`, default: `dlq`)
- `invalidChannel` (string) — topic for invalid messages (defaults to `%task%_invalid`)

### 6.4 Dead Letter Router (category: route)

Routes messages based on a field value. Maps field values to downstream topic
names.

**Config:**
- `field` (string, required) — payload field to route on
- `routes` (map: value → topicSuffix) — routing table
- `defaultRoute` (string) — fallback topic suffix

### 6.5 KV Enricher (category: enrich)

Looks up an external key and merges the result into the payload.

**Config:**
- `keyField` (string, required) — which field in the payload is the lookup key
- `source` (enum: `kafka_topic`, `http`, `inline`, default: `inline`)
- `sourceConfig` (map) — source-specific configuration
- `mergeStrategy` (enum: `shallow`, `deep`, `prefix`, default: `shallow`)
- `errorStrategy` (enum, default: `skip`)

### 6.6 Window Counter (category: aggregate)

Counts messages arriving in a time window and emits a summary record.

**Config:**
- `windowSeconds` (integer, required)
- `groupBy` (string, optional) — field to group counts by
- `emitOnClose` (boolean, default: true)

---

## 7. Directory Layout for Generated Data Pipelines

A generated data pipeline project would look like:

```
generated/
├── src/main/java/org/gautelis/durga/generated/
│   ├── workers/
│   │   ├── TransformInvoiceWorker.java      # plugin executor
│   │   ├── ValidateInvoiceWorker.java       # plugin executor
│   │   └── EnrichInvoiceWorker.java         # plugin executor
│   ├── gateways/
│   │   └── XorInvoiceApprovedService.java
│   ├── orchestrator/
│   │   └── ProcessOrchestratorService.java
│   └── starter/
│       └── InvoiceProcessingStarter.java
├── src/main/resources/
│   └── application.yml
├── config/
│   └── pipeline-config.yml                  # plugin configuration overrides
├── schemas/
│   ├── invoice-input.json                   # JSON Schema
│   └── invoice-output.json                  # JSON Schema
├── topics.sh
├── summary.json
└── README.md
```

---

## 8. Migration Path

The plugin system is additive. Existing BPMN models without plugin attributes
continue to generate generic workers as before.

1. **Phase 1 (now):** Reference implementations of core plugins with manual BPMN annotation.
2. **Phase 2:** Plugin registry YAML format, descriptor loading in scaffolder, validation.
3. **Phase 3:** Template-based plugin executor generation.
4. **Phase 4:** Plugin SDK for third-party plugin development.
5. **Phase 5:** Visual modeler integration (annotate BPMN tasks via Camunda Modeler palette).
