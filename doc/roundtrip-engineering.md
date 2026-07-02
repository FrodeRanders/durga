# Round-Trip Engineering for Custom Pipeline Code

## The problem

When you generate a data pipeline from BPMN, most activities are
covered by pre-built plugins — the scaffolder produces a complete,
working worker. But some activities need custom logic that no plugin
can express. You have three unpalatable choices:

1. **Embed code in the BPMN model** (as a `ScriptTask` or Camunda
   extension property). This code can't be compiled, tested, or
   refactored in an IDE. It lives in XML.

2. **Modify the generated Java file directly.** It works once. On
   regeneration, your changes are either overwritten or protected by
   fragile marker comments.

3. **Write a new plugin.** Overkill for a one-off transformation that
   only exists in one pipeline.

What you want is to write normal Java code in a normal IDE, have it
compiled and tested, and have the model *learn* about it so that
future regenerations preserve the connection.

## The proposed workflow

```
┌──────────────────────────────────────────────────────────────────────┐
│ 1. MODEL                                                             │
│                                                                      │
│ Draw the pipeline in Camunda Modeler. For activities that need       │
│ custom code, set:                                                    │
│                                                                      │
│   camunda:plugin="custom"                                            │
│   camunda:pluginConfig="interface=org.example.MyTransform"           │
│                                                                      │
│ No code in the model. Just a marker saying "this will be custom".    │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 2. GENERATE                                                          │
│                                                                      │
│ durga pipeline.bpmn --out generated/                                 │
│                                                                      │
│ Produces:                                                            │
│   generated/                                                         │
│   ├── src/main/java/example/                                         │
│   │   ├── TransformWorker.java        (plugin: generated, complete)  │
│   │   ├── FilterWorker.java           (plugin: generated, complete)  │
│   │   ├── MyTransformContract.java    (custom: interface skeleton)   │
│   │   └── org/gautelis/durga/         (runtime support classes)      │
│   ├── src/main/resources/                                            │
│   │   └── pipeline.bpmn                ← model copied as companion   │
│   └── pom.xml                                                        │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 3. IMPLEMENT                                                         │
│                                                                      │
│ Developer writes:                                                    │
│                                                                      │
│   // MyTransformImpl.java                                            │
│   public class MyTransformImpl implements MyTransformContract {      │
│       public byte[] execute(byte[] payload, String config) {         │
│           return customLogic(payload);                               │
│       }                                                              │
│   }                                                                  │
│                                                                      │
│ Compiles. Runs tests. Uses normal tools. Nothing generated is        │
│ modified — the custom code lives in a separate file that implements  │
│ the generated contract.                                              │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 4. BUILD → MODEL ENRICHMENT                                          │
│                                                                      │
│ mvn package triggers a Maven plugin that:                            │
│                                                                      │
│   a) Scans the compiled classes for implementations of generated     │
│      contracts (MyTransformContract).                                │
│   b) Finds the matching BPMN activity (by contract name → task id).  │
│   c) Writes implementation metadata back into the local copy of      │
│      the BPMN model:                                                 │
│                                                                      │
│      <camunda:property name="plugin" value="custom" />               │
│      <camunda:property name="pluginConfig"                           │
│         value="interface=org.example.MyTransformContract" />         │
│      <camunda:property name="customImpl"                             │
│         value="org.example.MyTransformImpl" />        ← written here │
│      <camunda:property name="customSource"                           │
│         value="MyTransformImpl.java" />               ← written here │
│                                                                      │
│   d) Optionally emits a hash of the implementation for change        │
│      detection.                                                      │
│                                                                      │
│ The enriched model now captures the connection between the BPMN      │
│ activity and the hand-written implementation class.                  │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 5. VERSION CONTROL                                                   │
│                                                                      │
│ git commit includes:                                                 │
│   - Generated contract interfaces (regenerated on model change)      │
│   - Hand-written implementation classes (never touched by Durga)     │
│   - Enriched BPMN model (now contains customImpl references)         │
│   - Plugin worker classes (fully generated, can be .gitignored)      │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│ 6. REGENERATION                                                      │
│                                                                      │
│ When the pipeline needs to change:                                   │
│                                                                      │
│   a) Start from the enriched BPMN model. It already knows:           │
│      - Which activities use plugins (complete generation)            │
│      - Which activities are custom (generate contract only)          │
│      - Which implementation class backs each custom activity         │
│      - Which source file contains the implementation                 │
│                                                                      │
│   b) Run durga pipeline.bpmn --out generated/                        │
│                                                                      │
│   c) The scaffolder:                                                 │
│      - Regenerates plugin workers from scratch                       │
│      - Regenerates contract interfaces for custom activities         │
│      - Skips generating contracts that already exist and match       │
│      - Preserves customImpl/customSource/customHash properties       │
│      - Restores embedded custom sources (durga:source) to            │
│        src/main/java when they are not already present locally        │
│      - Emits a warning if a referenced implementation class no       │
│        longer exists (the developer deleted it)                      │
│                                                                      │
│ The hand-written implementation classes survive untouched, and a     │
│ BPMN carried on its own can regenerate the whole project including    │
│ its locally written implementations.                                 │
└──────────────────────────────────────────────────────────────────────┘
```

## Why this beats embedding code in the model

| Property | Embedded script (ScriptTask) | Round-trip with contracts |
|---|---|---|
| **IDE support** | None. Code lives in XML. | Full — Java files in a normal package. |
| **Compilation** | Not compiled. Runtime eval. | Compiled by Maven, errors caught at build time. |
| **Testing** | Can't unit test XML text. | Standard JUnit tests on implementation classes. |
| **Refactoring** | Find-and-replace in XML. | IDE refactoring tools work. |
| **Dependencies** | Can't import libraries. | Normal Maven dependencies. |
| **Debugging** | Opaque eval stack trace. | Normal Java debugger, breakpoints. |
| **Code review** | XML diff of embedded code. | Normal Java file diff. |
| **Model cleanliness** | Code clutters the diagram. | Model only carries a reference. |
| **Regeneration safety** | Embedded code survives in XML | Implementation class never touched by generator. |

## The contract interface

For a custom activity, the scaffolder generates a contract that the
developer implements:

```java
// Generated: MyTransformContract.java
// Corresponds to BPMN activity "transform_data" in pipeline.bpmn
// Regenerated when the activity configuration changes.
// Do not modify this file — implement MyTransformImpl instead.

package org.example;

import org.gautelis.durga.plugins.Plugin;

/**
 * Contract for custom activity "transform_data".
 *
 * Implement this interface and annotate your class with
 * the implementation details. The pipeline worker delegates
 * to your implementation at runtime.
 */
public interface MyTransformContract extends Plugin {
    // Extends Plugin — provides default byte[] and String execute methods
}
```

The generated worker for the custom activity looks like:

```java
// Generated: TransformDataWorker.java
// Regenerated on model change.

@ApplicationScoped
public class TransformDataWorker {
    // Injected at runtime via CDI — the implementation class
    // is resolved from the BPMN model's customImpl property.
    @Inject
    MyTransformContract implementation;

    @Incoming("pipeline_transform_data_input")
    public CompletionStage<Void> handle(Message<String> msg) {
        // ... boilerplate, cancellation check, DLQ ...
        byte[] output = PluginExecutionSupport.execute(implementation, payload, config);
        // ... emit output, process-events, Vannak metadata event ...
    }
}
```

The generated worker parses custom activity output the same way as registered
plugin output. Returned JSON objects become the next event payload. JSON scalars,
arrays, and non-JSON text are wrapped under `{ "_value": ... }`. Returning `null`
leaves the incoming payload unchanged.

Custom workers and registered plugin workers share the same handle-aware
execution path. `handleMode=manual` passes `DataHandle` JSON to the implementation;
`handleMode=materialize` reads the referenced object-store bytes, runs the
implementation, writes the output back to the store, and forwards a new handle.
On successful execution, generated workers also emit a
`DataIndividualMetadataEvent` to `vannak-metadata-events`.

The developer writes:

```java
// Hand-written: MyTransformImpl.java
// Never touched by the scaffolder.

@ApplicationScoped
public class MyTransformImpl implements MyTransformContract {
    @Override
    public byte[] execute(byte[] payload, String config) {
        // Custom logic here — full Java, any library, testable.
        return transformedPayload;
    }
}
```

## Build-time model enrichment in detail

A Maven plugin (`durga-enrich-maven-plugin`) runs during `process-classes`:

```
mvn process-classes
  └── durga-enrich:enrich
        ├── Scans target/classes/ for implementations of generated contracts
        ├── Reads src/main/resources/pipeline.bpmn
        ├── Matches implementations to BPMN activities by contract name
        ├── For each match, writes/updates:
        │     <camunda:property name="customImpl" value="FQN" />
        │     <camunda:property name="customSource" value="path" />
        │     <camunda:property name="customHash" value="sha256" />
        ├── Embeds the implementation source plus its in-project
        │   support code (resolved by import-graph reachability) as
        │   CDATA payloads, each carrying its own content hash:
        │     <durga:source path="org/.../Impl.java" hash="sha256">
        │       <![CDATA[ ... ]]></durga:source>
        └── Writes the enriched model back to src/main/resources/pipeline.bpmn
            (whitespace-normalized; the only non-generated reference removed)
```

The enrichment is **additive and non-destructive** to model structure.
It runs on every build, so the model stays in sync with the code. The
local source is authoritative: when it diverges from the embedded copy,
the embedded copy and hash are refreshed and a warning is emitted
(local wins).

Each `durga:source` carries its own `hash`, and `customHash` is the
Merkle root over those per-file hashes (sorted by path). So a change to
*any* embedded file — including a transitive support file, not just the
implementation class — moves `customHash` and triggers re-embedding,
while the per-file hashes allow precise diagnostics and integrity
verification of each block independently. Because the source itself
travels inside the model, a BPMN file alone is enough to regenerate the
project: on regeneration the scaffolder restores any `durga:source`
files that are missing locally (and warns if a block fails its hash).

### Single source of truth for the enricher

`ModelEnricher` exists only as a StringTemplate (`modelEnricherClass`
in `scaffold-generated-project.stg`). The same template both (a) emits
the enricher into generated projects and (b) is rendered into
`target/generated-sources` during Durga's own build (via the
`gmavenplus` + `build-helper` plugins) so it can be compiled and tested
— there is no duplicated hand-written copy.

## Version control discipline

What to commit:

```
src/main/java/example/
├── TransformWorker.java        ← generated plugin worker (.gitignore ok)
├── FilterWorker.java           ← generated plugin worker (.gitignore ok)
├── MyTransformContract.java    ← GENERATED CONTRACT — COMMIT
└── MyTransformImpl.java        ← HAND-WRITTEN — COMMIT

src/main/resources/
└── pipeline.bpmn               ← ENRICHED MODEL — COMMIT
```

Plugin workers can be `.gitignored` because they are fully regenerated
from the BPMN model and contain no custom code. Contract interfaces
should be committed because they define the API between generated and
hand-written code. The enriched model must be committed because it is
the source of truth for regeneration.

If you `.gitignore` plugin workers, CI builds the pipeline after
checkout with:

```bash
durga pipeline.bpmn --out .
mvn package
```

## What changes trigger what

| Change | Action |
|---|---|
| Add/remove a plugin-driven activity in BPMN | Regenerate. New workers appear, old ones vanish. |
| Change plugin config in BPMN | Regenerate. Worker updates to new config. |
| Add a new custom activity in BPMN | Regenerate → new contract appears. Developer writes implementation. Build enriches model. |
| Change a custom activity's contract (e.g., new config schema) | Regenerate → contract changes. Developer updates implementation. Build re-enriches. |
| Modify custom implementation logic | Edit in IDE. Compile, test, build. Model enrichment updates the hash. No regeneration needed. |
| Reorder pipeline steps in BPMN | Regenerate. All plugin workers update. Custom contracts preserved. |
| Delete a custom activity from BPMN | Regenerate. Contract removed. Developer deletes orphaned implementation. |

## What needs to be built to make this work

| Component | Effort | Description |
|---|---|---|
| `camunda:plugin="custom"` handling in scaffolder | Small | Detect the marker, generate contract interface + delegating worker instead of plugin executor |
| Contract interface template | Small | A new `customContractClass` template in `scaffold.stg` |
| Delegating worker template | Small | A new `customDelegatingWorkerClass` template that injects the contract implementation via CDI |
| Model copy to generated project | Trivial | Already done for `summary.json` — copy the BPMN file to `src/main/resources/` |
| `durga-enrich-maven-plugin` | Medium | Scan classpath for contract implementations, match to BPMN activities, write metadata properties |
| Hash-based change detection in scaffolder | Small | On regeneration, read `customHash` from BPMN, compare with implementation class, emit warning |
| `.gitignore` template update | Trivial | Add generated plugin workers to the generated `.gitignore` |

## Relation to the existing plugin system

This does not replace plugins — it complements them. The decision tree
for an activity is:

```
Is there a plugin that covers this?
├── Yes → use camunda:plugin="<plugin-id>"
│         Fully generated. No code to write.
│
└── No  → use camunda:plugin="custom"
          ├── Contract interface generated
          ├── Developer implements contract
          ├── Build enriches model with implementation metadata
          └── Regeneration preserves the connection
```

The custom path uses the same `Plugin` interface so the contract
interface simply extends `Plugin`. This means custom implementations and registered plugins are
interchangeable at the contract level — a custom implementation can
later be promoted to a registered plugin without changing the BPMN model.
