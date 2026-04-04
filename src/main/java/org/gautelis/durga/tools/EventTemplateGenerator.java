package org.gautelis.durga.tools;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generates event-oriented runtime classes such as timers, message and signal handlers, boundary
 * handlers, and event publisher helpers.
 */
final class EventTemplateGenerator {
    private EventTemplateGenerator() {
    }

    /**
     * Generates intermediate timer catch handlers for simple one-in/one-out timer nodes.
     */
    static void generateTimerHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<TimerSpec> timerSpecs
    ) {
        for (TimerSpec timer : timerSpecs) {
            NodeInfo timerNode = nodes.get(timer.id);
            // Intermediate timer catch events are only scaffolded when they look like a simple
            // one-in/one-out step in the graph. More complex shapes need explicit BPMN support.
            if (timerNode == null || timerNode.incomingIds.size() != 1 || timerNode.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, timerNode.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, timerNode.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(timer.name) + "TimerService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("timerCatchEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("inputChannel", inputChannel.get());
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            template.add("timerId", timer.name);
            template.add("timerType", timer.timerType);
            template.add("timerExpression", timer.expression);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    /**
     * Generates waiting handlers for intermediate message catch events.
     */
    static void generateMessageCatchHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<MessageCatchSpec> messageCatchSpecs
    ) {
        for (MessageCatchSpec spec : messageCatchSpecs) {
            NodeInfo node = nodes.get(spec.id);
            // Catch events become waiting handlers: one upstream entry point, one downstream
            // continuation, and one external topic that resumes the flow.
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "MessageCatchService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("messageCatchEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("inputChannel", inputChannel.get());
            template.add("messageTopic", spec.topic);
            template.add("messageId", spec.name);
            template.add("messageName", spec.messageName);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    /**
     * Generates handlers for intermediate message throw events.
     */
    static void generateMessageThrowHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<MessageThrowSpec> messageThrowSpecs
    ) {
        for (MessageThrowSpec spec : messageThrowSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "MessageThrowService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("messageThrowEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("inputChannel", inputChannel.get());
            template.add("messageTopic", spec.topic);
            template.add("messageId", spec.name);
            template.add("messageName", spec.messageName);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    /**
     * Generates a helper CLI publisher for externally triggering message events.
     */
    static void generateMessageEventPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = BpmnScaffolder.toClassName(processId) + "MessageEventPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("messageEventPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            BpmnScaffolder.writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    /**
     * Generates a helper CLI publisher for externally triggering signal events.
     */
    static void generateSignalEventPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = BpmnScaffolder.toClassName(processId) + "SignalEventPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("signalEventPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            BpmnScaffolder.writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    /**
     * Generates waiting handlers for intermediate signal catch events.
     */
    static void generateSignalCatchHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<SignalCatchSpec> signalCatchSpecs
    ) {
        for (SignalCatchSpec spec : signalCatchSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "SignalCatchService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("signalCatchEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("inputChannel", inputChannel.get());
            template.add("signalTopic", spec.topic);
            template.add("signalId", spec.name);
            template.add("signalName", spec.signalName);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    /**
     * Generates handlers for intermediate signal throw events.
     */
    static void generateSignalThrowHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<SignalThrowSpec> signalThrowSpecs
    ) {
        for (SignalThrowSpec spec : signalThrowSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "SignalThrowService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("signalThrowEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("inputChannel", inputChannel.get());
            template.add("signalTopic", spec.topic);
            template.add("signalId", spec.name);
            template.add("signalName", spec.signalName);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    static void generateBoundaryErrorHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<BoundaryErrorSpec> boundaryErrorSpecs
    ) {
        for (BoundaryErrorSpec spec : boundaryErrorSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "BoundaryErrorService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("boundaryErrorEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("attachedTaskId", spec.attachedTaskId);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            template.add("errorId", spec.name);
            template.add("errorCode", spec.errorCode);
            template.add("cancelActivity", spec.cancelActivity);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    static void generateBoundaryEscalationHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<BoundaryEscalationSpec> boundaryEscalationSpecs
    ) {
        for (BoundaryEscalationSpec spec : boundaryEscalationSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "BoundaryEscalationService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("boundaryEscalationEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("attachedTaskId", spec.attachedTaskId);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            template.add("escalationId", spec.name);
            template.add("escalationCode", spec.escalationCode);
            template.add("cancelActivity", spec.cancelActivity);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    static void generateBoundaryTimerHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<BoundaryTimerSpec> boundaryTimerSpecs
    ) {
        for (BoundaryTimerSpec spec : boundaryTimerSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "BoundaryTimerService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("boundaryTimerEventClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("attachedTaskId", spec.attachedTaskId);
            template.add("outputChannel", output.get().channel);
            template.add("outputActivityId", output.get().taskId);
            template.add("outputNodeType", output.get().nodeType.code);
            template.add("timerId", spec.name);
            template.add("timerType", spec.timerType);
            template.add("timerExpression", spec.expression);
            template.add("cancelActivity", spec.cancelActivity);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    static void generateCallActivityHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<CallActivitySpec> callActivitySpecs
    ) {
        for (CallActivitySpec spec : callActivitySpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = BpmnScaffolder.toClassName(spec.name) + "CallActivityService";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile)) {
                continue;
            }
            ST template = group.getInstanceOf("callActivityHandlerClass");
            template.add("packageName", "org.gautelis.durga.generated");
            template.add("className", className);
            template.add("processId", processId);
            template.add("taskId", spec.name);
            template.add("calledElement", spec.calledElement);
            template.add("inputChannel", inputChannel.get());
            template.add("replyChannel", processId + "_" + spec.name + "_reply");
            template.add("requestChannel", processId + "_" + spec.name + "_call");
            template.add("outputChannel", processId + "_" + spec.name + "_output");
            template.add("nextChannel", output.get().channel);
            template.add("nextActivityId", output.get().taskId);
            template.add("nextNodeType", output.get().nodeType.code);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }
}
