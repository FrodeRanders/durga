# Release Checklist

Use this checklist for release candidates and for any change that claims to move
Durga closer to beta readiness.

## Version And Scope

- Confirm the release version is not `1.0-SNAPSHOT`.
- Add or update the `CHANGES.md` entry for the release.
- Confirm the candidate currently resolves as `0.1.0-beta.1`.
- Confirm the supported BPMN scope still matches
  `doc/beta-support-boundary.md`.
- Record any intentionally unsupported BPMN semantics in
  `doc/bpmn-kafka-coverage.md`.

## Required Verification

Run and record the result of each gate:

```bash
cd monitoring-ui && npm ci && npm run build
cd ..
mvn test -Dtest='!*IntegrationTest'
mvn test -Dtest='*IntegrationTest' -Ddurga.integration.requireDocker=true
```

Record:

- command,
- date,
- machine or CI runner,
- JDK version,
- Node.js version,
- Docker version,
- test counts and failures/skips.

The Docker-backed integration gate must report zero skipped tests for a release
candidate.

## Generated Project Smoke

For at least one representative BPMN model:

```bash
mvn -q clean package
java -jar target/durga-0.1.0-beta.1.jar src/test/resources/bpmn/invoice_receipt.bpmn
cd generated
mvn test
```

For a plugin pipeline, also scaffold one of:

- `src/test/resources/bpmn/data_pipeline_demo.bpmn`
- `src/test/resources/bpmn/order_events_pipeline.bpmn`
- `src/test/resources/bpmn/log_processing_pipeline.bpmn`

## Operational Review

Before tagging a release candidate:

- Review topic retention and compaction assumptions against
  `doc/operations-hardening.md`.
- Confirm generated topic scripts use the intended partition and replication
  settings for the target environment.
- Confirm monitoring state-store recovery has been exercised by the integration
  suite.
- Confirm plaintext Kafka listeners are not presented as production defaults.
- Confirm helper scripts do not require privileged runtime access in production
  packaging.

## Release Evidence Template

```text
Release candidate:
Commit:
Date:
JDK:
Node:
Docker:

UI build:
Unit/generated-project tests:
Docker integration tests:
Representative scaffold smoke:
Plugin scaffold smoke:

Known limits:
Operational notes:
```
