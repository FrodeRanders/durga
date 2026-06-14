# Maturity Plan

This document records the current maturity target and the gates required to move
Durga from active prototype toward beta readiness.

## Current Classification

Durga is an active prototype with alpha-level engineering structure:

- the core Maven build is repeatable on JDK 21,
- the Java unit suite covers core contracts, plugins, monitoring projections, and
  generated-project compilation,
- Kafka-backed integration tests exist but require Docker,
- the monitoring UI has a small dedicated API-helper test suite and production
  build gate,
- BPMN support is broad but intentionally scoped.

The project should not be described as production-ready until the beta gates
below are satisfied.

Operational assumptions and remaining hardening work are tracked in
[`operations-hardening.md`](operations-hardening.md).

The beta BPMN compatibility boundary is tracked in
[`beta-support-boundary.md`](beta-support-boundary.md).

Release-candidate evidence is tracked with
[`release-checklist.md`](release-checklist.md).

## Quality Gates

Run these gates before merging changes that affect generated code, Kafka
runtime behavior, monitoring, or plugins:

```bash
# Java unit and generated-project checks, no Docker required
mvn test -Dtest='!*IntegrationTest'

# Monitoring UI tests and build
cd monitoring-ui && npm ci && npm test && npm run build

# Kafka/Testcontainers verification, Docker required
mvn test -Dtest='*IntegrationTest' -Ddurga.integration.requireDocker=true
```

For release candidates, run the full Maven verification after the UI has been
built:

```bash
mvn verify -Ddurga.integration.requireDocker=true
```

## Beta Readiness Criteria

Durga can be considered beta-ready when the following are true:

1. CI runs both the monitoring UI test/build gate and the Maven verification gate.
2. Integration tests are mandatory for release candidates and documented as
   Docker-dependent.
3. The supported BPMN subset is treated as a compatibility contract.
4. Multi-instance loop behavior is either implemented with generated runtime
   handlers or explicitly marked out of scope for beta.
5. Top-level timer, error, and escalation event subprocess semantics are either
   implemented or explicitly marked out of scope for beta.
6. Operational behavior is documented for Kafka topic retention, state-store
   recovery, security assumptions, and upgrade compatibility.
7. Release artifacts use non-SNAPSHOT versions and each release has a changelog
   entry.
8. Release candidates record UI, unit, integration, and scaffold-smoke evidence.

## Known Prototype Limits

- `ScriptTask` and `BusinessRuleTask` generate stubs that require user code.
- `ComplexGateway` has no dedicated mapping.
- BPMN data objects and stores are only partially represented through payloads
  and state maps.
- Advanced BPMN scope semantics are approximated.
- Timer, error, and escalation event subprocess starts are supported inside
  embedded subprocess scopes, not as generalized top-level process semantics.
- Monitoring history is intentionally coarse.

## Next Implementation Priorities

1. Keep integration test execution visible in release instructions.
2. Maintain the beta support boundary as BPMN and runtime coverage changes.
3. Keep operations hardening guidance aligned with retention, state recovery,
   security, and upgrade compatibility changes.
4. Expand frontend tests as monitoring UI behavior grows beyond the current
   dashboard API-helper surface.
