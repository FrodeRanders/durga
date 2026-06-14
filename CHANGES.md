# Changes

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
- **pii-mask** — masks sensitive fields with configurable char and boundary preservation
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
- BPMN fixtures moved from `src/main/resources/bpmn/` to `src/test/resources/bpmn/`
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
