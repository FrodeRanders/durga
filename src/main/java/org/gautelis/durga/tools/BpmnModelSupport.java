package org.gautelis.durga.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal BPMN graph node used while the scaffolder resolves channels and generated handlers.
 */
class NodeInfo {
    final String id;
    final String name;
    final NodeType type;
    final TaskKind taskKind;
    // The scaffolder builds a lightweight in-memory graph model first and resolves channels
    // from that graph later during generation.
    final List<String> incomingIds = new ArrayList<>();
    final List<String> outgoingIds = new ArrayList<>();
    String defaultFlowId;

    NodeInfo(String id, String name, NodeType type) {
        this(id, name, type, null);
    }

    NodeInfo(String id, String name, NodeType type, TaskKind taskKind) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.taskKind = taskKind;
    }
}

/**
 * Internal task descriptor extracted from the BPMN model before code generation starts.
 */
final class TaskSpec {
    final String id;
    final String name;
    final TaskKind kind;
    final String pluginRef;
    final String pluginConfig;
    final String pluginImplClass;

    TaskSpec(String id, String name, TaskKind kind) {
        this(id, name, kind, null, null, null);
    }

    TaskSpec(String id, String name, TaskKind kind, String pluginRef, String pluginConfig) {
        this(id, name, kind, pluginRef, pluginConfig, null);
    }

    TaskSpec(String id, String name, TaskKind kind, String pluginRef, String pluginConfig,
             String pluginImplClass) {
        this.id = id;
        this.name = name;
        this.kind = kind;
        this.pluginRef = pluginRef;
        this.pluginConfig = pluginConfig;
        this.pluginImplClass = pluginImplClass;
    }
}

/**
 * Supported task families in the generated runtime.
 */
enum TaskKind {
    SERVICE("serviceTask"),
    USER("userTask"),
    MANUAL("manualTask"),
    SEND("sendTask"),
    RECEIVE("receiveTask"),
    SCRIPT("scriptTask"),
    BUSINESS_RULE("businessRuleTask"),
    CALL("callActivity"),
    PLUGIN("pluginTask"),
    GENERIC("task");

    final String bpmnType;

    TaskKind(String bpmnType) {
        this.bpmnType = bpmnType;
    }
}

/**
 * Internal node categories used when mapping BPMN graph elements to generated runtime behavior.
 */
enum NodeType {
    TASK("task"),
    CALL_ACTIVITY("callActivity"),
    SUB_PROCESS("subProcess"),
    XOR("xorGateway"),
    OR("inclusiveGateway"),
    AND("parallelGateway"),
    START("startEvent"),
    END("endEvent"),
    TIMER("intermediateCatchEvent"),
    BOUNDARY_TIMER("boundaryEvent"),
    BOUNDARY_ERROR("boundaryEvent"),
    BOUNDARY_ESCALATION("boundaryEvent"),
    MESSAGE_CATCH("intermediateCatchEvent"),
    MESSAGE_THROW("intermediateThrowEvent"),
    SIGNAL_CATCH("intermediateCatchEvent"),
    SIGNAL_THROW("intermediateThrowEvent");

    final String code;

    NodeType(String code) {
        this.code = code;
    }
}

/**
 * Resolved outgoing edge used as the template-facing routing model.
 * <p>
 * This is intentionally simple and reflection-friendly because StringTemplate accesses it
 * directly when rendering generated gateway and event-handler code.
 */
class OutputSpec {
    // OutputSpec is passed straight into the StringTemplate layer, so these fields stay public
    // to keep the generated template model simple and reflection-friendly.
    public final String emitter;
    public final String channel;
    public final String taskId;
    public final NodeType nodeType;
    public final String condition;
    public final boolean isDefault;
    public final String conditionBlock;

    public OutputSpec(
            String emitter,
            String channel,
            String taskId,
            NodeType nodeType,
            String condition,
            boolean isDefault,
            String conditionBlock
    ) {
        this.emitter = emitter;
        this.channel = channel;
        this.taskId = taskId;
        this.nodeType = nodeType;
        this.condition = condition;
        this.isDefault = isDefault;
        this.conditionBlock = conditionBlock;
    }
}

/**
 * Internal descriptor for simple intermediate timer catch events.
 */
final class TimerSpec {
    final String id;
    final String name;
    final String timerType;
    final String expression;

    TimerSpec(String id, String name, String timerType, String expression) {
        this.id = id;
        this.name = name;
        this.timerType = timerType;
        this.expression = expression;
    }
}

/**
 * Internal descriptor for boundary timer events attached to a task or subprocess.
 */
final class BoundaryTimerSpec {
    final String id;
    final String name;
    final String timerType;
    final String expression;
    final String attachedTaskId;
    final boolean cancelActivity;

    BoundaryTimerSpec(
            String id,
            String name,
            String timerType,
            String expression,
            String attachedTaskId,
            boolean cancelActivity
    ) {
        this.id = id;
        this.name = name;
        this.timerType = timerType;
        this.expression = expression;
        this.attachedTaskId = attachedTaskId;
        this.cancelActivity = cancelActivity;
    }
}

/**
 * Internal descriptor for boundary error events attached to a task or subprocess.
 */
final class BoundaryErrorSpec {
    final String id;
    final String name;
    final String attachedTaskId;
    final String errorCode;
    final boolean cancelActivity;

    BoundaryErrorSpec(
            String id,
            String name,
            String attachedTaskId,
            String errorCode,
            boolean cancelActivity
    ) {
        this.id = id;
        this.name = name;
        this.attachedTaskId = attachedTaskId;
        this.errorCode = errorCode;
        this.cancelActivity = cancelActivity;
    }
}

/**
 * Internal descriptor for boundary escalation events attached to a task or subprocess.
 */
final class BoundaryEscalationSpec {
    final String id;
    final String name;
    final String attachedTaskId;
    final String escalationCode;
    final boolean cancelActivity;

    BoundaryEscalationSpec(
            String id,
            String name,
            String attachedTaskId,
            String escalationCode,
            boolean cancelActivity
    ) {
        this.id = id;
        this.name = name;
        this.attachedTaskId = attachedTaskId;
        this.escalationCode = escalationCode;
        this.cancelActivity = cancelActivity;
    }
}

/**
 * Internal descriptor for intermediate message catch events.
 */
final class MessageCatchSpec {
    final String id;
    final String name;
    final String messageName;
    final String topic;

    MessageCatchSpec(String id, String name, String messageName, String topic) {
        this.id = id;
        this.name = name;
        this.messageName = messageName;
        this.topic = topic;
    }
}

/**
 * Internal descriptor for intermediate message throw events.
 */
final class MessageThrowSpec {
    final String id;
    final String name;
    final String messageName;
    final String topic;

    MessageThrowSpec(String id, String name, String messageName, String topic) {
        this.id = id;
        this.name = name;
        this.messageName = messageName;
        this.topic = topic;
    }
}

/**
 * Internal descriptor for intermediate signal catch events.
 */
final class SignalCatchSpec {
    final String id;
    final String name;
    final String signalName;
    final String topic;

    SignalCatchSpec(String id, String name, String signalName, String topic) {
        this.id = id;
        this.name = name;
        this.signalName = signalName;
        this.topic = topic;
    }
}

/**
 * Internal descriptor for intermediate signal throw events.
 */
final class SignalThrowSpec {
    final String id;
    final String name;
    final String signalName;
    final String topic;

    SignalThrowSpec(String id, String name, String signalName, String topic) {
        this.id = id;
        this.name = name;
        this.signalName = signalName;
        this.topic = topic;
    }
}

/**
 * Internal descriptor for BPMN call activities.
 */
final class CallActivitySpec {
    final String id;
    final String name;
    final String calledElement;

    CallActivitySpec(String id, String name, String calledElement) {
        this.id = id;
        this.name = name;
        this.calledElement = calledElement;
    }
}

/**
 * Internal descriptor for embedded subprocess scopes.
 */
final class SubProcessSpec {
    final String id;
    final String name;
    final List<String> entryTargetIds;
    final List<String> exitSourceIds;
    final List<String> scopeActivityIds;

    SubProcessSpec(
            String id,
            String name,
            List<String> entryTargetIds,
            List<String> exitSourceIds,
            List<String> scopeActivityIds
    ) {
        this.id = id;
        this.name = name;
        this.entryTargetIds = entryTargetIds;
        this.exitSourceIds = exitSourceIds;
        this.scopeActivityIds = scopeActivityIds;
    }
}

/**
 * Trigger families supported for generated event subprocesses.
 */
enum EventTriggerKind {
    // Event subprocesses are grouped by how the generated runtime wakes them up:
    // external topics, timers, or scoped lifecycle-monitor signals.
    MESSAGE,
    SIGNAL,
    TIMER,
    ERROR,
    ESCALATION
}

/**
 * Internal descriptor for generated event subprocess handlers.
 */
final class EventSubProcessSpec {
    final String id;
    final String name;
    final EventTriggerKind triggerKind;
    final String triggerName;
    final String triggerTopic;
    final String timerType;
    final String timerExpression;
    final String enclosingScopeActivityId;
    final boolean interrupting;
    final List<String> cancellationScopeActivityIds;
    final List<String> entryTargetIds;
    final List<String> exitSourceIds;
    final List<String> scopeActivityIds;

    EventSubProcessSpec(
            String id,
            String name,
            EventTriggerKind triggerKind,
            String triggerName,
            String triggerTopic,
            String timerType,
            String timerExpression,
            String enclosingScopeActivityId,
            boolean interrupting,
            List<String> cancellationScopeActivityIds,
            List<String> entryTargetIds,
            List<String> exitSourceIds,
            List<String> scopeActivityIds
    ) {
        this.id = id;
        this.name = name;
        this.triggerKind = triggerKind;
        this.triggerName = triggerName;
        this.triggerTopic = triggerTopic;
        this.timerType = timerType;
        this.timerExpression = timerExpression;
        this.enclosingScopeActivityId = enclosingScopeActivityId;
        this.interrupting = interrupting;
        this.cancellationScopeActivityIds = cancellationScopeActivityIds;
        this.entryTargetIds = entryTargetIds;
        this.exitSourceIds = exitSourceIds;
        this.scopeActivityIds = scopeActivityIds;
    }
}

/**
 * Template-facing description of one generated {@code @Incoming} method for joins,
 * orchestrators, or subprocess completion handlers.
 */
final class JoinMethodSpec {
    // JoinMethodSpec exists to render explicit @Incoming methods in generated code rather than
    // hiding channel fan-in behind loops or indirection.
    final String method;
    final String channel;
    final String activityId;
    final String completionActivityId;

    JoinMethodSpec(String method, String channel, String activityId, String completionActivityId) {
        this.method = method;
        this.channel = channel;
        this.activityId = activityId;
        this.completionActivityId = completionActivityId;
    }

    JoinMethodSpec(String method, String channel, String activityId) {
        this(method, channel, activityId, activityId);
    }
}

/**
 * Internal representation of one sequence-flow edge after BPMN parsing.
 */
final class FlowInfo {
    final String id;
    final String targetId;
    final String condition;

    FlowInfo(String id, String targetId, String condition) {
        this.id = id;
        this.targetId = targetId;
        this.condition = condition;
    }
}

/**
 * Parsed scaffolder command-line arguments.
 */
final class ParsedArgs {
    final String bpmnPath;
    final String outputDir;
    final boolean dryRun;
    final boolean transactions;
    final boolean separateWorkers;
    final boolean connect;
    final boolean strimzi;

    ParsedArgs(String bpmnPath, String outputDir, boolean dryRun, boolean transactions,
               boolean separateWorkers, boolean connect, boolean strimzi) {
        this.bpmnPath = bpmnPath;
        this.outputDir = outputDir;
        this.dryRun = dryRun;
        this.transactions = transactions;
        this.separateWorkers = separateWorkers;
        this.connect = connect;
        this.strimzi = strimzi;
    }
}

/**
 * Internal descriptor for a BPMN send task with a message reference.
 */
final class SendTaskSpec {
    final String id;
    final String name;
    final String messageName;

    SendTaskSpec(String id, String name, String messageName) {
        this.id = id;
        this.name = name;
        this.messageName = messageName;
    }
}

/**
 * Internal descriptor for a BPMN receive task with a message reference.
 */
final class ReceiveTaskSpec {
    final String id;
    final String name;
    final String messageName;

    ReceiveTaskSpec(String id, String name, String messageName) {
        this.id = id;
        this.name = name;
        this.messageName = messageName;
    }
}

/**
 * Minimal descriptor for multi-instance loop characteristics on a task or subprocess.
 */
final class MultiInstanceSpec {
    final String taskId;
    final String taskName;
    final boolean sequential;
    final String loopCardinality;
    final String completionCondition;

    MultiInstanceSpec(String taskId, String taskName, boolean sequential, String loopCardinality, String completionCondition) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.sequential = sequential;
        this.loopCardinality = loopCardinality;
        this.completionCondition = completionCondition;
    }
}
