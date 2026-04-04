# BPMN to Kafka Coverage

This matrix describes the current execution mapping in `durga`. It is a support matrix for the scaffolder, not a claim of full BPMN coverage.

| BPMN element | Kafka mapping in `durga` | Status | Notes |
| --- | --- | --- | --- |
| `StartEvent` | Producer to `%processId%_start` plus canonical `process-events` lifecycle emission | Supported | Generated starter classes cover this path. |
| `Task` | Consumer on `%processId%_%taskId%_input`, producer on `%processId%_%taskId%_output`, lifecycle emission to `process-events` | Supported | Generic task semantics only. |
| `ExclusiveGateway` | Consumer on upstream topic, conditional producer to one downstream task input, `GATEWAY_TAKEN` lifecycle emission | Supported | Conditions are generated as placeholders. |
| `ParallelGateway` split | Consumer on upstream topic, fan-out producer to multiple downstream task inputs, lifecycle emission | Supported | Simple fan-out behavior. |
| `ParallelGateway` join | Consumers on multiple upstream outputs, state-topic coordination, producer to next task input, lifecycle emission | Supported | Uses `ProcessStateStore` to coordinate joins. |
| Terminal completion | Consumer on terminal task outputs, orchestrator emits `PROCESS_COMPLETED` | Supported | Completion is explicit in lifecycle events. |
| `EndEvent` | Explicit completion activity emission from terminal task outputs into `PROCESS_COMPLETED` | Supported | End-event ids/names are now propagated into the lifecycle stream. |
| `ServiceTask` | Auto-completing worker on task input/output topics | Supported | This is the current generated worker model. |
| `UserTask` | Waiting-state consumer plus external completion publisher | Supported | Use generated `complete-task.sh` to advance the task. |
| `ManualTask` | Waiting-state consumer plus external completion publisher | Supported | Uses the same external completion pattern as user tasks. |
| `IntermediateCatchEvent` with timer | Delayed handoff from upstream flow into next task or end event | Supported | Current support covers intermediate catch timers with a single inbound and outbound sequence flow. |
| `IntermediateCatchEvent` with message | Wait-state consumer plus external message topic resume | Supported | Use generated `send-message-event.sh` to resume the catch event. |
| `IntermediateThrowEvent` with message | Publishes to an external message topic and continues the flow | Supported | Generated throw handlers emit both the message topic and lifecycle events. |
| `IntermediateCatchEvent` with signal | Wait-state consumer plus external signal topic resume | Supported | Use generated `send-signal-event.sh` to resume the catch event. |
| `IntermediateThrowEvent` with signal | Publishes to an external signal topic and continues the flow | Supported | Generated throw handlers emit both the signal topic and lifecycle events. |
| `BoundaryEvent` with timer | Timeout watcher on `process-events` plus alternate flow handoff | Supported | Works for tasks and subprocess scopes with a single outgoing boundary flow. Interrupting and non-interrupting timer boundaries are both generated from BPMN `cancelActivity`. |
| `BoundaryEvent` with error | Watches failed scope/task events on `process-events` and routes into the boundary flow | Supported | Works for tasks and subprocess scopes with a single outgoing boundary flow. |
| `BoundaryEvent` with escalation | Watches escalated scope/task events on `process-events` and routes into the boundary flow | Supported | Works for tasks and subprocess scopes with a single outgoing boundary flow. |
| `CallActivity` | Wait-state activity with `%processId%_%callActivity%_call` request topic and `%processId%_%callActivity%_reply` completion topic | Supported | Generated helpers include `complete-call-activity.sh` and a call-activity completion publisher. |
| Embedded `SubProcess` | Generated scope entry/completion services emit subprocess lifecycle events and route into and out of the internal graph | Supported | Works for nested subprocesses as well, using subprocess input/output channels between scopes. |
| Event `SubProcess` with message/signal start | Generated event-topic listener emits subprocess lifecycle and starts the internal scope graph | Partial | Current support covers message or signal start events. Non-interrupting starts branch without cancellation; interrupting starts emit cancellation for the enclosing scope before starting the event subprocess flow. |
| Event `SubProcess` with timer start | Generated timer watcher on enclosing subprocess lifecycle emits subprocess lifecycle and starts the internal scope graph after the delay | Partial | Current support covers timer starts inside an embedded subprocess scope. |
| Event `SubProcess` with error/escalation start | Generated process-event watcher listens for scoped `FAILED` or `ESCALATED` lifecycle events and starts the internal scope graph | Partial | Current support covers error and escalation starts inside an embedded subprocess scope. |
| `InclusiveGateway` / `ComplexGateway` | No dedicated mapping yet | Not supported | Current control-flow support is XOR and AND only. |
| BPMN data objects / stores | Generic event payloads and state maps | Partial | No BPMN-native data model projection yet. |

## Current executable subset

The practical executable subset today is:

- process start
- generic tasks
- user and manual tasks with external completion
- call activities with external completion
- timer catch events
- message catch and throw events
- escalation, timer, and error boundary events
- XOR routing
- AND split/join
- embedded subprocess scopes with generated enter/complete services
- message/signal event subprocesses
- timer event subprocesses inside embedded subprocess scopes
- error/escalation event subprocesses inside embedded subprocess scopes
- process completion
- monitoring via canonical `process-events`

## Recommended implementation order

The next coverage steps with the best payoff are:

1. Decide whether top-level process timer event subprocess starts need first-class semantics.
2. Decide whether top-level error/escalation event subprocess starts need first-class semantics.
3. Add inclusive and complex gateway support if the execution subset needs to expand.

## Demo and observability surface

Generated projects now include runnable helpers for learning and testing the mapping:

- `demo-scenario.sh`
- `send-task-input.sh`
- `complete-task.sh`
- `fail-task.sh`
- `escalate-task.sh`
- `complete-call-activity.sh`
- `send-message-event.sh`
- `send-signal-event.sh`
- `watch-process-events.sh`
- `watch-task-output.sh`
- `task-payloads.json`

That means the current support boundary is inspectable from both the producer side and the consumer side.

## Non-interrupting boundary behavior

`durga` now has explicit sample coverage for non-interrupting timer boundaries via `src/main/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn` and `src/main/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn`.

The generated boundary watcher respects BPMN `cancelActivity="false"`:

- it emits the reminder branch when the timer fires
- it does not emit `ACTIVITY_CANCELLED` for the attached activity
- the original activity remains eligible to complete through its normal path
- the same behavior applies when the attached scope is an embedded subprocess
