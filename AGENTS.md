# Repository Guidelines

## Project Structure & Module Organization
- `src/main/java/org/gautelis/durga/tools/` contains the BPMN scaffolder (entry point: `BpmnScaffolder`).
- `src/main/java/org/gautelis/durga/monitoring/` holds Kafka Streams monitoring topology, HTTP server/client, and query services.
- `src/main/java/org/gautelis/durga/demo/` contains demo publishers and scenario runners.
- `src/main/java/org/gautelis/durga/` holds core runtime contracts (`ProcessEvent`, `ProcessState`, `ScopeCancellationRegistry`, `ProcessStateStore`).
- `src/main/resources/` holds runtime configuration (`application.yml`) and logging (`log4j2.xml`).
- `src/test/resources/bpmn/` contains BPMN example models used as scaffolder test fixtures.
- `setup/` contains local Kafka infrastructure (`docker-compose.yml`, `config.yml`).
- `target/` is Maven build output (generated).

## Build, Test, and Development Commands
- `mvn clean package` builds the shaded JAR and cleans logs configured by the clean plugin.
- `mvn test` runs JUnit 4 tests (currently 61 tests across 5 test classes).
- `mvn compile` compiles sources without running tests.
- `mvn dependency:tree` prints the resolved dependency graph.
- `mvn -Dtest=XxxTest test` runs a single test class.
- `java -jar target/durga-1.0-SNAPSHOT.jar <path-to-bpmn.xml>` runs the packaged BPMN scaffolder.
- `cd setup && docker compose up` starts a local Kafka broker and Kafka UI for development.
- `mvn -Pdeps validate` prints dependency update information.

## Coding Style & Naming Conventions
- Java uses 4-space indentation with standard brace placement (match existing files).
- Package names are lowercase (`org.gautelis.durga`); classes use UpperCamelCase; methods/fields use lowerCamelCase.
- Keep configuration keys in `application.yml` and logging in `log4j2.xml`; avoid hardcoding broker settings.

## Testing Guidelines
- JUnit 4 is configured in `pom.xml`; place tests under `src/test/java/` with names like `XxxTest`.
- Prefer focused unit tests for services; use Kafka-in-container tests only when needed.

## Commit & Pull Request Guidelines
- Git history shows short, lowercase commit subjects (e.g., `upd deps`, `playing with transactions`). Follow that lightweight style.
- PRs should include: a clear purpose, key changes, and local run instructions (commands + environment notes). Add screenshots only if UI behavior changes.

## Configuration & Local Kafka Tips
- Kafka connection details are managed through `setup/` and `application.yml`. Keep dev defaults in `setup/` and document any new ports.
- Expect the Kafka UI to log transient connection errors until the broker is ready.
