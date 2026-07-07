package org.gautelis.durga.tools;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generates subprocess and event-subprocess runtime handlers.
 * <p>
 * These templates are responsible for turning subprocess boundaries into explicit generated
 * entry/completion services rather than flattening everything into one opaque runtime.
 */
final class SubProcessTemplateGenerator {
    private SubProcessTemplateGenerator() {
    }

    /**
     * Generates entry and completion handlers for embedded subprocess scopes.
     */
    static void generateSubProcessHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<SubProcessSpec> subProcessSpecs
    ) {
        for (SubProcessSpec spec : subProcessSpecs) {
            NodeInfo node = nodes.get(spec.id);
            if (node == null || spec.entryTargetIds.isEmpty()) {
                continue;
            }

            List<String> directIncomingChannels = new ArrayList<>();
            for (String sourceId : node.incomingIds) {
                Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, sourceId);
                inputChannel.ifPresent(channel -> {
                    if (!directIncomingChannels.contains(channel)) {
                        directIncomingChannels.add(channel);
                    }
                });
            }

            List<OutputSpec> entryOutputs =
                    BpmnScaffolder.resolveOutputChannels(processId, nodes, spec.entryTargetIds, null, null);
            if (entryOutputs.isEmpty()) {
                continue;
            }

            String entryClassName = BpmnScaffolder.toClassName(spec.name) + "SubProcessEntryService";
            Path entryOutputFile = javaOutput.resolve(entryClassName + ".java");
            if (!Files.exists(entryOutputFile)) {
                // Entry handlers convert "arrive at the subprocess boundary" into "start the
                // internal subprocess graph" while emitting scope lifecycle for monitoring.
                ST entryTemplate = group.getInstanceOf("subProcessEntryHandlerClass");
                entryTemplate.add("packageName", BpmnScaffolder.generatedPackage);
                entryTemplate.add("className", entryClassName);
                entryTemplate.add("subProcessId", spec.name);
                entryTemplate.add("entryChannel", processId + "_" + spec.name + "_input");
                entryTemplate.add("incomingMethodsBlock", buildSubProcessEntryMethodsBlock(spec.name, directIncomingChannels));
                entryTemplate.add("entryOutputs", entryOutputs);
                if (!dryRun) {
                    BpmnScaffolder.writeFile(entryOutputFile, entryTemplate.render());
                }
                generatedFiles.add(outputRoot.relativize(entryOutputFile).toString());
            }

            List<JoinMethodSpec> completionInputs = new ArrayList<>();
            for (String exitSourceId : spec.exitSourceIds) {
                NodeInfo exitSource = nodes.get(exitSourceId);
                Optional<String> channel = BpmnScaffolder.resolveInputChannel(processId, nodes, exitSourceId);
                if (exitSource != null && channel.isPresent()) {
                    completionInputs.add(new JoinMethodSpec(
                            "on" + BpmnScaffolder.toClassName(exitSource.name),
                            channel.get(),
                            exitSource.name,
                            spec.name
                    ));
                }
            }
            if (completionInputs.isEmpty()) {
                continue;
            }

            Optional<OutputSpec> outerOutput = BpmnScaffolder.resolveSingleOutput(processId, nodes, node.outgoingIds);
            String completionClassName = BpmnScaffolder.toClassName(spec.name) + "SubProcessCompletionService";
            Path completionOutputFile = javaOutput.resolve(completionClassName + ".java");
            if (Files.exists(completionOutputFile)) {
                continue;
            }
            // Completion handlers collapse multiple internal exits back into one subprocess-level
            // completion signal before the outer graph continues.
            ST completionTemplate = group.getInstanceOf("subProcessCompletionHandlerClass");
            completionTemplate.add("packageName", BpmnScaffolder.generatedPackage);
            completionTemplate.add("className", completionClassName);
            completionTemplate.add("subProcessId", spec.name);
            completionTemplate.add("scopeActivityIds", spec.scopeActivityIds);
            completionTemplate.add("incomingMethodsBlock", buildOrchestratorMethodsBlock(completionInputs));
            completionTemplate.add("completionChannel", processId + "_" + spec.name + "_output_emit");
            completionTemplate.add("outputChannel", outerOutput.map(output -> output.channel).orElse("lifecycle-events"));
            completionTemplate.add("outputActivityId", outerOutput.map(output -> output.taskId).orElse("completed"));
            completionTemplate.add("outputNodeType", outerOutput.map(output -> output.nodeType.code).orElse("endEvent"));
            if (!dryRun) {
                BpmnScaffolder.writeFile(completionOutputFile, completionTemplate.render());
            }
            generatedFiles.add(outputRoot.relativize(completionOutputFile).toString());
        }
    }

    /**
     * Generates start and completion handlers for event subprocesses across supported trigger
     * families.
     */
    static void generateEventSubProcessHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            Map<String, NodeInfo> nodes,
            List<EventSubProcessSpec> eventSubProcessSpecs
    ) {
        for (EventSubProcessSpec spec : eventSubProcessSpecs) {
            if (spec.entryTargetIds.isEmpty()) {
                continue;
            }

            List<OutputSpec> entryOutputs =
                    BpmnScaffolder.resolveOutputChannels(processId, nodes, spec.entryTargetIds, null, null);
            if (entryOutputs.isEmpty()) {
                continue;
            }

            String entryClassName = BpmnScaffolder.toClassName(spec.name) + "EventSubProcessStartService";
            Path entryOutputFile = javaOutput.resolve(entryClassName + ".java");
            if (!Files.exists(entryOutputFile)) {
                boolean timerTrigger = spec.triggerKind == EventTriggerKind.TIMER;
                boolean monitorTrigger = spec.triggerKind == EventTriggerKind.ERROR
                        || spec.triggerKind == EventTriggerKind.ESCALATION;
                // Event subprocesses have three trigger families in the generated runtime:
                // external topics, scoped timers, and scoped lifecycle-monitor events.
                ST entryTemplate = group.getInstanceOf(
                        timerTrigger
                                ? "eventSubProcessTimerStartHandlerClass"
                                : monitorTrigger
                                ? "eventSubProcessMonitorStartHandlerClass"
                                : "eventSubProcessStartHandlerClass"
                );
                entryTemplate.add("packageName", BpmnScaffolder.generatedPackage);
                entryTemplate.add("className", entryClassName);
                entryTemplate.add("subProcessId", spec.name);
                entryTemplate.add("interrupting", spec.interrupting);
                entryTemplate.add("cancellationScopeActivityIds", spec.cancellationScopeActivityIds);
                entryTemplate.add("scopeActivityIds", spec.scopeActivityIds);
                entryTemplate.add("entryOutputs", entryOutputs);
                if (timerTrigger) {
                    entryTemplate.add("timerType", spec.timerType);
                    entryTemplate.add("timerExpression", spec.timerExpression);
                    entryTemplate.add("enclosingScopeActivityId", spec.enclosingScopeActivityId);
                } else if (monitorTrigger) {
                    entryTemplate.add("triggerKind", spec.triggerKind.name());
                    entryTemplate.add("triggerCode", spec.triggerName);
                    entryTemplate.add("enclosingScopeActivityId", spec.enclosingScopeActivityId);
                } else {
                    entryTemplate.add("triggerTopic", spec.triggerTopic);
                    entryTemplate.add("triggerKind", spec.triggerKind.name());
                }
                if (!dryRun) {
                    BpmnScaffolder.writeFile(entryOutputFile, entryTemplate.render());
                }
                generatedFiles.add(outputRoot.relativize(entryOutputFile).toString());
            }

            List<JoinMethodSpec> completionInputs = new ArrayList<>();
            for (String exitSourceId : spec.exitSourceIds) {
                NodeInfo exitSource = nodes.get(exitSourceId);
                Optional<String> channel = BpmnScaffolder.resolveInputChannel(processId, nodes, exitSourceId);
                if (exitSource != null && channel.isPresent()) {
                    completionInputs.add(new JoinMethodSpec(
                            "on" + BpmnScaffolder.toClassName(exitSource.name),
                            channel.get(),
                            exitSource.name,
                            spec.name
                    ));
                }
            }
            if (completionInputs.isEmpty()) {
                continue;
            }

            String completionClassName = BpmnScaffolder.toClassName(spec.name) + "EventSubProcessCompletionService";
            Path completionOutputFile = javaOutput.resolve(completionClassName + ".java");
            if (Files.exists(completionOutputFile)) {
                continue;
            }
            ST completionTemplate = group.getInstanceOf("eventSubProcessCompletionHandlerClass");
            completionTemplate.add("packageName", BpmnScaffolder.generatedPackage);
            completionTemplate.add("className", completionClassName);
            completionTemplate.add("subProcessId", spec.name);
            completionTemplate.add("scopeActivityIds", spec.scopeActivityIds);
            completionTemplate.add("incomingMethodsBlock", buildOrchestratorMethodsBlock(completionInputs));
            if (!dryRun) {
                BpmnScaffolder.writeFile(completionOutputFile, completionTemplate.render());
            }
            generatedFiles.add(outputRoot.relativize(completionOutputFile).toString());
        }
    }

    /**
     * Renders explicit completion methods for generated orchestrator-like handlers.
     *
     * @param incomingMethods method specifications to render
     * @return Java source snippet containing concrete {@code @Incoming} methods
     */
    static String buildOrchestratorMethodsBlock(List<JoinMethodSpec> incomingMethods) {
        StringBuilder builder = new StringBuilder();
        for (JoinMethodSpec method : incomingMethods) {
            // Render concrete @Incoming methods instead of a loop in the template so the
            // generated class stays explicit and easy to inspect in an IDE.
            // @Blocking runs the handler on a worker thread: handleCompletion polls Kafka via
            // the shared ProcessStateStore, which must not run on the reactive event loop and
            // must not be entered concurrently from multiple event-loop threads.
            builder.append("    @Incoming(\"")
                    .append(method.channel)
                    .append("\")\n")
                    .append("    @io.smallrye.common.annotation.Blocking\n")
                    .append("    public CompletionStage<Void> ")
                    .append(method.method)
                    .append("(Message<String> msg) {\n")
                    .append("        return handleCompletion(\"")
                    .append(method.completionActivityId)
                    .append("\", msg);\n")
                    .append("    }\n\n");
        }
        return builder.toString();
    }

    private static String buildSubProcessEntryMethodsBlock(String subProcessId, List<String> channels) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < channels.size(); i++) {
            String channel = channels.get(i);
            builder.append("    @Incoming(\"")
                    .append(channel)
                    .append("\")\n")
                    .append("    public CompletionStage<Void> on")
                    .append(BpmnScaffolder.toClassName(subProcessId))
                    .append("Entry")
                    .append(i + 1)
                    .append("(Message<String> msg) {\n")
                    .append("        return enterScope(msg);\n")
                    .append("    }\n\n");
        }
        return builder.toString();
    }
}
