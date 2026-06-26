# Repository Guidelines

## Project Structure & Module Organization
- `src/main/java/org/gautelis/durga/tools/` contains the BPMN scaffolder (entry point: `BpmnScaffolder`). Shared spec types are in `BpmnModelSupport.java`; model element collection is in `BpmnModelCollector.java`; CLI argument parsing is in `CliParser.java`; code generation helpers are in `EventTemplateGenerator.java`, `TaskRoutingGenerator.java`, `SubProcessTemplateGenerator.java`, and `GeneratedProjectSupport.java`.
- `src/main/java/org/gautelis/durga/monitoring/` holds Kafka Streams monitoring topology, Quarkus REST API (`ProcessMonitoringApp`), CLI client, query service, and component-level metrics (`Metrics.java`).
- `src/main/java/org/gautelis/durga/plugins/` contains 15 data pipeline plugins implementing the `Plugin` interface, plus shared utilities (`PipelinePlugin.java`).
- `src/main/java/org/gautelis/durga/demo/` contains demo publishers and scenario runners.
- `src/main/java/org/gautelis/durga/` holds core runtime contracts (`ProcessEvent`, `ProcessState`, `ScopeCancellationRegistry`, `ProcessStateStore`).
- `src/main/resources/templates/` holds StringTemplate 4 `.stg` template files used by the scaffolder.
- `src/test/resources/bpmn/` contains BPMN example models used as scaffolder test fixtures (24 models covering all BPMN 2.0 element types).
- `setup/` contains local Kafka infrastructure (`docker-compose.yml`, `config.yml`).
- `target/` is Maven build output (generated).

## Build, Test, and Development Commands
- `mvn clean package` builds the shaded JAR and cleans logs configured by the clean plugin.
- `mvn test` runs all JUnit 4 tests. As of 2026-06-26, `mvn test -Dtest='!*IntegrationTest'` runs 219 non-Docker tests; integration tests require Docker and are suffixed `IntegrationTest`.
- `mvn test -Dtest='!*IntegrationTest'` runs unit tests only (no Docker required).
- `mvn test -Dtest='*IntegrationTest'` runs integration tests only (requires Docker).
- `mvn compile` compiles sources without running tests.
- `mvn dependency:tree` prints the resolved dependency graph.
- `mvn -Dtest=XxxTest test` runs a single test class.
- `java -jar target/durga-0.1.0-beta.1.jar <path-to-bpmn.xml>` runs the packaged BPMN scaffolder.
- `cd setup && docker compose up` starts a local Kafka broker and Kafka UI for development.
- `mvn -Pdeps validate` prints dependency update information.

## Coding Style & Naming Conventions
- Java uses 4-space indentation with standard brace placement (match existing files).
- Package names are lowercase (`org.gautelis.durga`); classes use UpperCamelCase; methods/fields use lowerCamelCase.
- Use SLF4J (`LoggerFactory.getLogger`) for logging — do not use `System.out`/`System.err` for log messages.
- Component-level observability uses Micrometer (`Metrics.registry()`). Expose monitoring metrics via the `/api/metrics` HTTP endpoint.
- Keep configuration keys in `application.yml` and logging in `log4j2.xml`; avoid hardcoding broker settings.

## Testing Guidelines
- JUnit 4 is configured in `pom.xml`; place tests under `src/test/java/` with names like `XxxTest`.
- Integration tests use Testcontainers (`KafkaIntegrationTestBase`); extend that base class for any test needing a real Kafka broker.
- Prefer focused unit tests for services; use Kafka-in-container tests for topology, state store, and serialization verification.
- Each test method should include `System.out.println("TC: description")` for traceability.

## Commit & Pull Request Guidelines
- Git history shows short, lowercase commit subjects (e.g., `upd deps`, `playing with transactions`). Follow that lightweight style.
- PRs should include: a clear purpose, key changes, and local run instructions (commands + environment notes). Add screenshots only if UI behavior changes.

## Configuration & Local Kafka Tips
- Kafka connection details are managed through `setup/` and `application.yml`. Keep dev defaults in `setup/` and document any new ports.
- Expect the Kafka UI to log transient connection errors until the broker is ready.
