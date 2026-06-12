# Changes

## [Current] — 2026-06-12

### BPMN coverage — 6 new elements
- **SendTask** — dedicated Kafka producer handler
- **ReceiveTask** — dedicated Kafka consumer wait-state handler
- **ScriptTask** — stub worker with `// TODO: implement script logic`
- **BusinessRuleTask** — stub worker with `// TODO: implement business rule`
- **InclusiveGateway** — OR split (all matching conditions) and OR join (fire-on-first)
- **MultiInstanceLoopCharacteristics** — detected and reported in generation summary

### Data pipeline plugin architecture
- **`Plugin` interface** — `String execute(String payload, String config)` contract
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
