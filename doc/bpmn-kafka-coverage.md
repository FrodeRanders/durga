# BPMN to Kafka Coverage

This matrix describes the current execution mapping in `durga`. It is a support matrix for the scaffolder, not a claim of full BPMN coverage. The beta compatibility boundary is defined separately in [`beta-support-boundary.md`](beta-support-boundary.md).

| BPMN element | Kafka mapping in `durga` | Status | Notes |
| --- | --- | --- | --- |
| `StartEvent` | Producer to `%processId%_start` plus canonical `process-events-{processId}` lifecycle emission | Supported | Generated starter classes cover this path. |
| `ServiceTask` | Auto-completing worker on task input/output topics | Supported | Current generated worker model. |
| `UserTask` | Waiting-state consumer plus external completion publisher | Supported | Use generated `complete-task.sh` to advance. |
| `ManualTask` | Waiting-state consumer plus external completion publisher | Supported | Same external completion pattern as user tasks. |
| `SendTask` | Auto-completing worker that also publishes to `%processId%_%taskId%_send` message topic | Supported | Maps naturally to a Kafka producer. |
| `ReceiveTask` | Wait-state consumer on `%processId%_%taskId%_receive` topic, forward on message arrival | Supported | Maps naturally to a Kafka consumer. |
| `ScriptTask` | Worker stub with `// TODO: implement script logic` placeholder | Supported | Stub includes TODO markers for user implementation. |
| `BusinessRuleTask` | Worker stub with `// TODO: implement business rule evaluation` placeholder | Supported | Stub includes TODO markers for DMN/rule engine integration. |
| `Task` (generic/unknown) | Auto-completing worker (catch-all) | Supported with warning | Unrecognised task subtypes emit a stderr warning. |
| `ExclusiveGateway` | Consumer on upstream topic, conditional producer to one downstream task input, `GATEWAY_TAKEN` lifecycle emission | Supported | Conditions from `conditionExpression` are evaluated at runtime via a recursive descent expression parser (`==`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`, `!`, parentheses, nested field access). |
| `InclusiveGateway` split | Consumer on upstream topic, conditional fan-out to ALL matching downstream outputs | Supported | Same expression evaluator as XOR; all matching conditions fire (unlike XOR which picks first match). |
| `InclusiveGateway` join | Consumers on multiple upstream outputs, activates when first branch arrives, `GATEWAY_TAKEN` emission | Supported | Fire-on-first-arrival join model with instance-level arrived-set tracking. |
| `ParallelGateway` split | Consumer on upstream topic, fan-out producer to all downstream task inputs | Supported | Simple fan-out behavior. |
| `ParallelGateway` join | Consumers on multiple upstream outputs, state-topic coordination, producer to next task input | Supported | Uses `ProcessStateStore` to coordinate joins. All branches must arrive. |
| Terminal completion | Consumer on terminal task outputs, orchestrator emits `PROCESS_COMPLETED` | Supported | Completion is explicit in lifecycle events. |
| `EndEvent` | Explicit completion activity emission from terminal task outputs | Supported | End-event ids/names propagated into the lifecycle stream. |
| `IntermediateCatchEvent` with timer | Delayed handoff from upstream flow into next task or end event | Supported | Single inbound and outbound sequence flow. |
| `IntermediateCatchEvent` with message | Wait-state consumer plus external message topic resume | Supported | Use `send-message-event.sh` to resume. |
| `IntermediateThrowEvent` with message | Publishes to external message topic and continues flow | Supported | Emits both message topic and lifecycle events. |
| `IntermediateCatchEvent` with signal | Wait-state consumer plus external signal topic resume | Supported | Use `send-signal-event.sh` to resume. |
| `IntermediateThrowEvent` with signal | Publishes to external signal topic and continues flow | Supported | Emits both signal topic and lifecycle events. |
| `BoundaryEvent` with timer | Timeout watcher on `process-events-{processId}` plus alternate flow handoff | Supported | Tasks and subprocess scopes. Interrupting/non-interrupting. |
| `BoundaryEvent` with error | Watches failed scope/task events and routes into boundary flow | Supported | Tasks and subprocess scopes. |
| `BoundaryEvent` with escalation | Watches escalated scope/task events and routes into boundary flow | Supported | Tasks and subprocess scopes. |
| `CallActivity` | Wait-state activity with call/reply topics | Supported | Generated helpers include `complete-call-activity.sh`. |
| Embedded `SubProcess` | Generated scope entry/completion services | Supported | Works for nested subprocesses. |
| Event `SubProcess` with message/signal start | Generated event-topic listener | Supported | Interrupting starts emit cancellation for enclosing scope. |
| Event `SubProcess` with timer start | Generated timer watcher on enclosing subprocess lifecycle | Supported | Timer starts inside embedded subprocess scope. |
| Event `SubProcess` with error/escalation start | Generated process-event watcher for scoped failure/escalation | Supported | Error/escalation starts inside embedded subprocess scope. |
| `MultiInstanceLoopCharacteristics` | Detected and noted in generation summary | Noted | Parallel/sequential multi-instance metadata is collected in `summary.json`. No dedicated runtime handler generation yet. |
| `ComplexGateway` | No dedicated mapping | Not supported | Niche pattern. Left for manual implementation. |
| BPMN data objects / stores | Generic event payloads and state maps | Partial | No BPMN-native data model projection yet. |

## Current executable subset

- process start
- service, user, manual, send, receive, script and business rule tasks
- generic tasks (with warning)
- user and manual tasks with external completion
- call activities with external completion
- timer catch events
- message catch and throw events
- escalation, timer, and error boundary events
- XOR routing
- OR routing (inclusive gateway split and join)
- AND split/join
- embedded subprocess scopes
- message/signal event subprocesses
- timer/error/escalation event subprocesses inside embedded subprocess scopes
- process completion
- monitoring via canonical `process-events-{processId}`

## Recommended implementation order

The next coverage steps with the best payoff are:

1. Generate dedicated runtime handlers for `MultiInstanceLoopCharacteristics` (parallel multi-instance maps well to Kafka fan-out).
2. Add top-level event subprocess semantics for timer, error and escalation starts.
3. Add `ComplexGateway` support if required by the execution subset.
