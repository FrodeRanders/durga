# Beta Support Boundary

This document defines the BPMN and runtime behavior that should be considered
inside the beta compatibility contract. It is narrower than the full set of BPMN
constructs that Durga can parse or report.

## Beta-Supported BPMN Runtime Semantics

The following behavior is eligible for beta support once the quality gates in
`maturity-plan.md` pass:

- process start and explicit process completion,
- service, user, manual, send, receive, script, business rule, and generic task
  scaffolding,
- external completion for user tasks, manual tasks, and call activities,
- message and signal catch/throw events,
- timer catch events with one inbound and one outbound sequence flow,
- interrupting and non-interrupting timer boundary events on tasks and embedded
  subprocesses,
- error and escalation boundary events on tasks and embedded subprocesses,
- exclusive gateway routing with generated condition evaluation,
- inclusive gateway split and current fire-on-first-arrival join behavior,
- parallel gateway split and all-branches-arrived join behavior,
- embedded subprocess entry/completion services, including nested subprocesses,
- message and signal event subprocess starts,
- timer, error, and escalation event subprocess starts inside embedded
  subprocess scopes,
- plugin-dispatched service tasks using bundled plugin descriptors,
- generated monitoring events consumed by the monitoring topology.

## Beta Out Of Scope

The following behavior should not block beta if it remains documented as out of
scope:

- dedicated runtime handlers for `MultiInstanceLoopCharacteristics`,
- top-level timer, error, and escalation event subprocess semantics,
- `ComplexGateway`,
- BPMN-native data objects and data stores,
- generalized BPMN transaction, compensation, and choreography semantics,
- production-grade plugin SDK guarantees beyond the current `Plugin` interface,
- exact BPMN engine equivalence for advanced scope propagation.

These items may still be parsed, detected, noted in `summary.json`, or
represented by generated stubs. They are not part of the beta runtime
compatibility contract unless this document is updated.

## Stubbed But Supported As Scaffolding

`ScriptTask` and `BusinessRuleTask` are supported as scaffolding patterns, not
as full execution engines:

- generated script task workers include a clear implementation point for user
  script logic,
- generated business rule task workers include a clear implementation point for
  DMN or rule-engine integration,
- generated projects should compile before users fill in those implementations.

## Compatibility Rules

For beta releases:

- generated topic names and channel names should remain stable for the same BPMN
  model and scaffolder flags,
- generated lifecycle event status names should remain backward compatible,
- new `ProcessEvent` JSON fields should be optional on first introduction,
- generated helper scripts should keep existing command names and required
  arguments unless a migration note is added,
- changes to supported BPMN semantics require test fixtures and generated-project
  compile coverage.

## Promotion Criteria For Out-Of-Scope Items

An out-of-scope feature can move into the beta contract when it has:

1. an entry in `doc/bpmn-kafka-coverage.md`,
2. at least one BPMN fixture under `durga-tools/src/test/resources/bpmn/`,
3. unit or generated-project tests that exercise the generated artifacts,
4. Docker-backed integration coverage when Kafka state, ordering, or restart
   behavior is material to correctness,
5. documentation of runtime semantics and known limits.
