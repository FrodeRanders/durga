# Repository Guidelines

## Project Structure & Module Organization
The project is a multi-module Maven build (parent aggregator `durga`, `pom` packaging) with three Java modules plus a Rust crate:
- `durga-runtime/` holds core runtime contracts (`org.gautelis.durga.ProcessEvent`, `ProcessState`, `ScopeCancellationRegistry`, `ProcessStateStore`), the 16 data pipeline plugins implementing the `Plugin` interface (`org.gautelis.durga.plugins/`, plus shared utilities `PipelinePlugin.java`, `PluginResult.java`, `PluginExecutionSupport.java`), component-level metrics (`org.gautelis.durga.monitoring.Metrics`), and validation-mode support (`org.gautelis.durga.validation/`).
- `durga-tools/` contains the BPMN scaffolder (entry point: `org.gautelis.durga.tools.BpmnScaffolder`). Shared spec types are in `BpmnModelSupport.java`; model element collection is in `BpmnModelCollector.java`; CLI argument parsing is in `CliParser.java`; code generation helpers are in `EventTemplateGenerator.java`, `TaskRoutingGenerator.java`, `SubProcessTemplateGenerator.java`, `GeneratedProjectSupport.java`, and `RustTargetGenerator.java`. Demo publishers and scenario runners are in `org.gautelis.durga.demo/`. Templates live under `durga-tools/src/main/resources/templates-java/` (StringTemplate 4 `.stg` files for the Java target) and `templates-rust/` (Rust target). BPMN test fixtures are in `durga-tools/src/test/resources/bpmn/` (25 models covering BPMN 2.0 element types).
- `durga-monitor/` holds the Kafka Streams monitoring topology, Quarkus REST API (`org.gautelis.durga.monitoring.ProcessMonitoringApp`), CLI client (`ProcessMonitoringClient`), query services, and validation topology.
- `durga-rust/` is a Cargo crate providing the Rust plugin runtime for the Rust code-generation target: the `Plugin` trait (binary/text/structured dispatch mirroring the Java interface), `PluginResult` with `OutputDisposition`, a wire-compatible `ProcessEvent`, and Rust ports of the pipeline plugins. Generated Rust workers depend on it directly; there are no adapters onto the Java plugins.
- `monitoring-ui/` is the Svelte dashboard (Vite dev server proxies `/api` to the monitor REST API, default `http://localhost:8081`).
- `setup/` contains local Kafka infrastructure (`docker-compose.yml`, `config.yml`).
- `target/` under each module is Maven build output (generated).

## Build, Test, and Development Commands
- `mvn clean package` builds all modules (the scaffolder shaded JAR and the monitor Quarkus runner) and cleans logs configured by the clean plugin.
- `mvn test` runs all JUnit 4 tests across modules. `mvn test -Dtest='!*IntegrationTest'` runs the non-Docker suite (326 tests as of 2026-07-06: durga-runtime 208, durga-tools 57, durga-monitor 61); integration tests require Docker and are suffixed `IntegrationTest`.
- `mvn test -Dtest='!*IntegrationTest'` runs unit tests only (no Docker required).
- `mvn test -Dtest='*IntegrationTest'` runs integration tests only (requires Docker).
- `mvn compile` compiles sources without running tests.
- `mvn dependency:tree` prints the resolved dependency graph.
- `mvn -Dtest=XxxTest test` runs a single test class.
- `java -jar durga-tools/target/durga-tools-0.1.0-beta.1.jar <path-to-bpmn.xml>` runs the packaged BPMN scaffolder.
- `java -jar durga-monitor/target/durga-monitor-0.1.0-beta.1-runner.jar` runs the monitoring app.
- `cd durga-rust && cargo test` runs the Rust runtime tests (62 as of 2026-07-06).
- `cd setup && docker compose up` starts a local Kafka broker and Kafka UI for development.

## Coding Style & Naming Conventions
- Java uses 4-space indentation with standard brace placement (match existing files).
- Package names are lowercase (`org.gautelis.durga`); classes use UpperCamelCase; methods/fields use lowerCamelCase.
- Use SLF4J (`LoggerFactory.getLogger`) for logging — do not use `System.out`/`System.err` for log messages.
- Component-level observability uses Micrometer (`Metrics.registry()`). Expose monitoring metrics via the `/api/metrics` HTTP endpoint.
- Keep configuration keys in `application.yml` and logging in `log4j2.xml`; avoid hardcoding broker settings.

## Testing Guidelines
- JUnit 4 is configured in `pom.xml`; place tests under the owning module's `src/test/java/` with names like `XxxTest`.
- Integration tests use Testcontainers (`KafkaIntegrationTestBase`); extend that base class for any test needing a real Kafka broker.
- Prefer focused unit tests for services; use Kafka-in-container tests for topology, state store, and serialization verification.
- Each test method should include `System.out.println("TC: description")` for traceability.

## Commit & Pull Request Guidelines
- Git history shows short, lowercase commit subjects (e.g., `upd deps`, `playing with transactions`). Follow that lightweight style.
- PRs should include: a clear purpose, key changes, and local run instructions (commands + environment notes). Add screenshots only if UI behavior changes.

## Configuration & Local Kafka Tips
- Kafka connection details are managed through `setup/` and `application.yml`. Keep dev defaults in `setup/` and document any new ports.
- Expect the Kafka UI to log transient connection errors until the broker is ready.
