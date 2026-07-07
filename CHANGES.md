# Changes

## [Unreleased] — 2026-07-07

### Validation mode redesign (supersedes the 2026-07-06 entry below)
- `--validation` now generates the **complete process as a shadow**, not additive
  shadow workers beside production. The shadow reads the **live production topics**
  through a dedicated consumer group (`{processId}-validation`), writes each task's
  output/DLQ to `-validation` topics, and emits lifecycle events to a single fixed
  **`validation-events`** topic — never to a real process topic
- Side-effect suppression is now explicit: a single
  `executeWithResult(payload, config, PluginExecutionContext)` method (the 2-arg form
  was removed; mirrored by `execute_with_result` in the Rust trait). Side-effecting
  plugins (e.g. `ObjectStoreCollector`) suppress external writes when
  `context.validationMode()` is true while still returning the response they would have
  produced. **Retired** `ValidationCandidateOutput` and `validation-candidate-outputs`
- Generated workers emit `ACTIVITY_ENTERED` before executing, so per-task latency is
  measurable for plugin/data-pipeline tasks
- `ValidationTopology` compares the shadow's `ACTIVITY_COMPLETED` (fixed
  `validation-events` topic) against production per `processId:activityId:instance`.
  A fixed candidate source avoids the Kafka Streams "topic unknown to the topology"
  crash that a pattern-subscribed comparator hits when a per-process validation topic
  appears after startup
- `FaultDetectionTopology` and the main topology exclude `process-events-*-validation`
  (pattern `process-events-(?!.*-validation).*`), so a running shadow never trips
  phantom stuck/cascade alarms or pollutes production state

### Java/Rust interchangeability
- The Rust target now uses the **same chaining-topic names as Java**
  (`<task>_output`, gateway `<target>_input`, `<pid>_start`) instead of `<task>_in`,
  making the two targets wire-interchangeable. A Rust shadow can validate a running
  Java process (and vice versa), task by task

### Monitoring
- New per-activity **throughput** read model (`ActivityThroughput`,
  `process-activity-throughput` store) and `GET /api/processes/{id}/throughput`
- Diagram overlays show per-task **statistics** (items processed + avg/p95 latency)
  with active alarms layered on top, instead of a single execution's state
- Dashboard Validation Report hides `EQUAL` comparisons by default (opt-in toggle),
  persists per-comparison expand/collapse across polls, and the bpmn.io watermark no
  longer fills the diagram

### Tooling & docs
- `scripts/run-e2e-validation.sh` runs a validation shadow alongside `run-e2e-test.sh`
- README and the LaTeX system manual updated to match (validation mode,
  naming conventions, code-generation targets, plugin architecture, monitoring)

## [Unreleased] — 2026-07-06

### Validation mode
- New opt-in `--validation` scaffolder flag generates a **shadow worker** per plugin
  task that runs a candidate implementation against real input on a **dedicated
  consumer group** (`{processId}_{taskId}_validation`), leaving the production input
  index untouched, with side effects suppressed and output diverted to
  `validation-candidate-outputs` instead of the task output topic
- Handled **per task**: each candidate is fed the production input for that task, so
  an early divergence never cascades into later-task comparisons
- Runtime foundation in `durga-runtime` (`org.gautelis.durga.validation`):
  `JsonComparison` (normalized diff with `*`/`[*]` ignore-paths),
  `ValidationCandidateOutput`, `ValidationResult` (`EQUAL`/`DIFF`/`PRIOR_MISSING`/
  `CANDIDATE_ERROR`), and `PluginExecutionSupport.executeSandboxed` (materialize-mode
  side-effect suppression with a synthetic, content-hashed handle)
- Monitor `ValidationTopology` merges candidate outputs with prior/production
  `ACTIVITY_COMPLETED` output (keyed `processId:activityId:processInstanceId`, robust
  to arrival order) and materializes `validation-results`; new `/api/validation/*`
  endpoints, dashboard **Validation Report** panel, and `durga_validation_*` metrics
- **Java and Rust parity**: both targets emit shadow workers; `durga-rust` gains
  `ValidationCandidateOutput` and `plan_validation_output`, and generated Rust
  projects add a `{taskId}_validation` binary using `run_validation_worker`
- Comparison ignore-paths configurable via `durga.validation.ignore.paths`; candidate
  version stamped via `DURGA_VALIDATION_CANDIDATE_VERSION`
- Documented in the system manual (new "Validation Mode" chapter) and README

## [Current] — 2026-06-13

### Maturity gates and release discipline
- Added `doc/maturity-plan.md` with beta-readiness criteria and required quality gates
- CI now builds the monitoring UI before Maven verification
- Docker-backed integration tests can be required with `-Ddurga.integration.requireDocker=true`
  or `DURGA_REQUIRE_DOCKER_TESTS=true`, preventing release gates from silently skipping Kafka tests
- Added `doc/operations-hardening.md` for topic retention, state-store recovery,
  security assumptions, and upgrade compatibility
- Added `doc/beta-support-boundary.md` to define the beta BPMN compatibility contract
- Added `doc/release-checklist.md` for release-candidate evidence capture

### 7 new data pipeline plugins
- **type-coercer** — coerces fields between string/int/long/double/decimal/boolean
- **string-template** — `${field}` token substitution with dot-notation field access
- **mask** — masks explicitly configured fields with configurable char and boundary preservation
- **regex-extract** — extracts named capture groups from a source field into the payload
- **json-flatten** — flattens nested JSON to dot-notation keys, or unflattens the reverse
- **uuid-inject** — injects UUIDs (v4 or time-based v1) into specified payload fields
- **timestamp-normalize** — converts between epoch_s/ms, ISO8601, RFC3339, and custom patterns

### Gateway condition expression evaluator
- Rewrote `evaluateCondition()` from a simple `==`/`!=` checker into a recursive descent
  parser supporting `>`, `<`, `>=`, `<=`, `&&`, `||`, `!`, parentheses, nested field
  access (`data.amount`), and numeric/string/boolean/null literals
- Evaluator is embedded in generated XOR and OR split gateway classes — no new runtime
  dependencies
- Generated README now states conditions are evaluated at runtime instead of calling
  them placeholders
- Both `xorGatewayClass` and `orSplitGatewayClass` templates include the full evaluator

### Catalog
- 15 plugins total (8 existing + 7 new), all registered in `plugins/catalog.yml`

### Tests — 133 → 188
- 55 new unit tests across 7 new plugin test classes

## [Previous] — 2026-06-12

### BPMN coverage — 6 new elements
- **SendTask** — dedicated Kafka producer handler
- **ReceiveTask** — dedicated Kafka consumer wait-state handler
- **ScriptTask** — stub worker with `// TODO: implement script logic`
- **BusinessRuleTask** — stub worker with `// TODO: implement business rule`
- **InclusiveGateway** — OR split (all matching conditions) and OR join (fire-on-first)
- **MultiInstanceLoopCharacteristics** — detected and reported in generation summary

### Data pipeline plugin architecture
- **`Plugin` interface** — dual-path: override `String execute(String, String)` for text/JSON or `byte[] execute(byte[], String)` for binary; UTF-8 conversion helpers
- **5 reference plugins** — JsonTransform, FieldFilter, KvEnricher, DeadLetterRouter, WindowCounter
- **Plugin registry** — YAML descriptors + catalog in `plugins/`, SnakeYAML-based loader
- **Scaffolder integration** — `camunda:plugin` + `camunda:pluginConfig` Camunda extension attributes on `ServiceTask`
- **Dynamic template** — `pluginExecutorClass` instantiates plugin via interface, no per-plugin code paths
- **Bundled into JAR** — `plugins/` as Maven resource, classpath fallback
- **Config format** — standard `key=value` pairs across all plugins

### Repository hygiene
- Removed incompatible Jackson 2.x `jackson-annotations` from pom.xml
- Upgraded maven-shade-plugin 3.2.0 → 3.6.0
- Replaced deprecated `<source>/<target>` with `<release>21`
- Log path changed from `.` to `target/`
- Removed empty `components/` directory
- Replaced stale `application.yml` with minimal config
- Removed unused `@ApplicationScoped` from `ScopeCancellationRegistry`
- Hardcoded `localhost:9094` externalized to `kafka.bootstrap.servers` system property
- BPMN fixtures moved from `src/main/resources/bpmn/` to `durga-tools/src/test/resources/bpmn/`
- Added `.editorconfig` and `.github/workflows/build.yml` (JDK 21 CI)

### Documentation
- System manual: 22 chapters, fixed FQN and stale paths, tightened prose, added plugin architecture section
- `doc/data-pipeline-blueprint.md` — full architecture design with category governance
- `doc/bpmn-kafka-coverage.md` — expanded matrix with 10 new entries
- `AGENTS.md` — corrected project structure, test counts, added commands
- `README.md` — tightened from 354 to 155 lines, replaced verbose BPMN catalog with compact table

### Tests — 61 → 108
- 25 unit tests for core runtime contracts (ProcessEvent, ProcessState, ScopeCancellationRegistry, ProcessStateView)
- 43 unit tests for plugin implementations
- 2 integration tests for plugin-annotated BPMN scaffolding
