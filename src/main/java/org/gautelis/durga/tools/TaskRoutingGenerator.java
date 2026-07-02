package org.gautelis.durga.tools;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Generates task workers, gateways, and the top-level process orchestrator.
 */
final class TaskRoutingGenerator {
    private TaskRoutingGenerator() {
    }

    /**
     * Generates task handler classes for the extracted BPMN task set.
     */
    static void generateTaskHandlers(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            boolean transactions,
            String processId,
            List<TaskSpec> taskSpecs,
            Set<String> existingSources,
            Map<String, BpmnScaffolder.TaskLineage> taskLineage
    ) {
        for (TaskSpec task : taskSpecs) {
            String className = BpmnScaffolder.toClassName(task.name)
                    + BpmnScaffolder.classSuffixForTask(task.kind, transactions);
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                continue;
            }

            if (task.kind == TaskKind.CUSTOM || task.kind == TaskKind.SCRIPT
                    || task.kind == TaskKind.BUSINESS_RULE) {
                generateCustomTask(group, javaOutput, outputRoot, generatedFiles,
                        dryRun, processId, task, className, existingSources, taskLineage);
                continue;
            }

            String templateName = BpmnScaffolder.templateForTask(task.kind, transactions);
            ST worker = group.getInstanceOf(templateName);
            worker.add("packageName", BpmnScaffolder.generatedPackage);
            worker.add("className", className);
            worker.add("processId", processId);
            worker.add("taskId", task.name);
            worker.add("taskType", task.kind.bpmnType);
            if ("transactionalWorkerClass".equals(templateName)) {
                worker.add("eventsTopic", BpmnScaffolder.eventsTopic);
            }
            if (task.pluginRef != null) {
                worker.add("pluginRef", task.pluginRef);
                worker.add("pluginConfig", task.pluginConfig != null ? task.pluginConfig : ".");
                worker.add("pluginImplClass", task.pluginImplClass);
            }
            addLineageToTemplate(worker, taskLineage, task.name);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, worker.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void addLineageToTemplate(ST worker, Map<String, BpmnScaffolder.TaskLineage> taskLineage, String taskName) {
        BpmnScaffolder.TaskLineage lineage = taskLineage.getOrDefault(taskName,
                new BpmnScaffolder.TaskLineage(null, null, null));
        worker.add("taskReads", toJavaListExpr(lineage.reads));
        worker.add("taskWrites", toJavaListExpr(lineage.writes));
        worker.add("taskStores", toJavaListExpr(lineage.stores));
        worker.add("hasLineage", !lineage.reads.isEmpty() || !lineage.writes.isEmpty() || !lineage.stores.isEmpty());
    }

    private static String toJavaListExpr(List<String> items) {
        if (items == null || items.isEmpty()) {
            return "java.util.List.of()";
        }
        StringBuilder sb = new StringBuilder("java.util.List.of(");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escapeJava(items.get(i))).append("\"");
        }
        sb.append(")");
        return sb.toString();
    }

    private static String escapeJava(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void generateCustomTask(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            TaskSpec task,
            String className,
            Set<String> existingSources,
            Map<String, BpmnScaffolder.TaskLineage> taskLineage
    ) {
        String contractSimpleName = task.customContract != null
                && task.customContract.contains(".")
                ? task.customContract.substring(task.customContract.lastIndexOf('.') + 1)
                : (task.customContract != null ? task.customContract : BpmnScaffolder.toClassName(task.name) + "Contract");

        Path contractFile = javaOutput.resolve(contractSimpleName + ".java");

        boolean contractExists = Files.exists(contractFile)
                || existingSources.contains(contractSimpleName + ".java");

        if (!contractExists) {
            ST contract = group.getInstanceOf("customContractClass");
            contract.add("packageName", BpmnScaffolder.generatedPackage);
            contract.add("className", contractSimpleName);
            contract.add("processId", processId);
            contract.add("taskId", task.id);
            contract.add("taskName", task.name);
            if (!dryRun) {
                BpmnScaffolder.writeFile(contractFile, contract.render());
            }
            generatedFiles.add(outputRoot.relativize(contractFile).toString());
        }

        Path workerFile = javaOutput.resolve(className + ".java");
        if (!Files.exists(workerFile)) {
            ST worker = group.getInstanceOf("customDelegatingWorkerClass");
            worker.add("packageName", BpmnScaffolder.generatedPackage);
            worker.add("className", className);
            worker.add("contractClassName", contractSimpleName);
            worker.add("processId", processId);
            worker.add("taskId", task.name);
            worker.add("taskType", task.kind.bpmnType);
            worker.add("pluginConfig", task.pluginConfig != null ? task.pluginConfig : ".");
            worker.add("customImpl", task.customImpl != null ? task.customImpl : "");
            worker.add("customHash", task.customHash != null ? task.customHash : "");
            addLineageToTemplate(worker, taskLineage, task.name);
            if (!dryRun) {
                BpmnScaffolder.writeFile(workerFile, worker.render());
            }
            generatedFiles.add(outputRoot.relativize(workerFile).toString());
        }
    }

    /**
     * Generates XOR and AND gateway handlers after the BPMN graph has been resolved to channels.
     */
    static void generateGateways(
            String processId,
            STGroupString group,
            Path javaOutput,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            List<NodeInfo> ors,
            Set<String> existingSources,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        for (NodeInfo xor : xors) {
            if (xor.outgoingIds.size() < 2 || xor.incomingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, xor.incomingIds.getFirst());
            List<OutputSpec> outputs = BpmnScaffolder.resolveOutputChannels(
                    processId, nodes, xor.outgoingIds, flowsBySource.get(xor.id), xor.defaultFlowId
            );
            if (inputChannel.isEmpty() || outputs.size() < 2) {
                continue;
            }
            outputs = BpmnScaffolder.withConditionBlocks(outputs);

            String className = "Xor" + BpmnScaffolder.toClassName(xor.name) + "Service";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                continue;
            }
            ST tmpl = group.getInstanceOf("xorGatewayClass");
            tmpl.add("packageName", BpmnScaffolder.generatedPackage);
            tmpl.add("className", className);
            tmpl.add("inputChannel", inputChannel.get());
            tmpl.add("outputs", outputs);
            if (!dryRun) {
                BpmnScaffolder.writeFile(outputFile, tmpl.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }

        for (NodeInfo and : ands) {
            if (and.outgoingIds.size() > 1 && and.incomingIds.size() == 1) {
                Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, and.incomingIds.getFirst());
                List<OutputSpec> outputs = BpmnScaffolder.resolveOutputChannels(processId, nodes, and.outgoingIds, null, null);
                if (inputChannel.isEmpty() || outputs.isEmpty()) {
                    continue;
                }
                String className = "And" + BpmnScaffolder.toClassName(and.name) + "SplitService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("andSplitGatewayClass");
                tmpl.add("packageName", BpmnScaffolder.generatedPackage);
                tmpl.add("className", className);
                tmpl.add("inputChannel", inputChannel.get());
                tmpl.add("outputs", outputs);
                if (!dryRun) {
                    BpmnScaffolder.writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            } else if (and.incomingIds.size() > 1) {
                List<JoinMethodSpec> incomingMethods = resolveIncomingMethods(processId, nodes, and.incomingIds);
                Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, and.outgoingIds);
                if (incomingMethods.isEmpty() || output.isEmpty()) {
                    continue;
                }
                String className = "And" + BpmnScaffolder.toClassName(and.name) + "JoinService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("andJoinGatewayClass");
                tmpl.add("packageName", BpmnScaffolder.generatedPackage);
                tmpl.add("className", className);
                tmpl.add("processId", processId);
                tmpl.add("outputChannel", output.get().channel);
                tmpl.add("outputTaskId", output.get().taskId);
                tmpl.add("incomingMethodsBlock", buildJoinMethodsBlock(incomingMethods));
                tmpl.add("joinCondition", buildJoinCondition(incomingMethods));
                if (!dryRun) {
                    BpmnScaffolder.writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            }
        }

        for (NodeInfo or : ors) {
            if (or.outgoingIds.size() >= 2 && or.incomingIds.size() == 1) {
                Optional<String> inputChannel = BpmnScaffolder.resolveInputChannel(processId, nodes, or.incomingIds.getFirst());
                List<OutputSpec> outputs = BpmnScaffolder.resolveOutputChannels(
                        processId, nodes, or.outgoingIds, flowsBySource.get(or.id), or.defaultFlowId
                );
                if (inputChannel.isEmpty() || outputs.size() < 2) {
                    continue;
                }
                outputs = BpmnScaffolder.withConditionBlocks(outputs);

                String className = "Or" + BpmnScaffolder.toClassName(or.name) + "SplitService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("orSplitGatewayClass");
                tmpl.add("packageName", BpmnScaffolder.generatedPackage);
                tmpl.add("className", className);
                tmpl.add("inputChannel", inputChannel.get());
                tmpl.add("outputs", outputs);
                if (!dryRun) {
                    BpmnScaffolder.writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            } else if (or.incomingIds.size() > 1) {
                List<JoinMethodSpec> incomingMethods = resolveIncomingMethods(processId, nodes, or.incomingIds);
                Optional<OutputSpec> output = BpmnScaffolder.resolveSingleOutput(processId, nodes, or.outgoingIds);
                if (incomingMethods.isEmpty() || output.isEmpty()) {
                    continue;
                }
                String className = "Or" + BpmnScaffolder.toClassName(or.name) + "JoinService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("orJoinGatewayClass");
                tmpl.add("packageName", BpmnScaffolder.generatedPackage);
                tmpl.add("className", className);
                tmpl.add("processId", processId);
                tmpl.add("outputChannel", output.get().channel);
                tmpl.add("outputTaskId", output.get().taskId);
                tmpl.add("incomingMethodsBlock", buildJoinMethodsBlock(incomingMethods));
                tmpl.add("incomingCount", incomingMethods.size());
                if (!dryRun) {
                    BpmnScaffolder.writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            }
        }
    }

    /**
     * Generates the top-level completion orchestrator for terminal task outputs.
     */
    static void generateOrchestrator(
            String processId,
            STGroupString group,
            Path javaOutput,
            Map<String, NodeInfo> nodes,
            Set<String> existingSources,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        List<JoinMethodSpec> incomingMethods = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type != NodeType.TASK && node.type != NodeType.CALL_ACTIVITY) {
                continue;
            }
            String endActivityId = resolveEndActivityId(nodes, node);
            if (endActivityId == null) {
                continue;
            }
            String channel = processId + "_" + node.name + "_output";
            incomingMethods.add(new JoinMethodSpec(
                    "on" + BpmnScaffolder.toClassName(node.name) + "Completed",
                    channel,
                    node.name,
                    endActivityId
            ));
        }
        if (incomingMethods.isEmpty()) {
            return;
        }
        String className = "ProcessOrchestratorService";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
            return;
        }
        ST tmpl = group.getInstanceOf("orchestratorClass");
        tmpl.add("packageName", BpmnScaffolder.generatedPackage);
        tmpl.add("className", className);
        tmpl.add("processId", processId);
        tmpl.add("incomingMethodsBlock", SubProcessTemplateGenerator.buildOrchestratorMethodsBlock(incomingMethods));
        if (!dryRun) {
            BpmnScaffolder.writeFile(outputFile, tmpl.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static List<JoinMethodSpec> resolveIncomingMethods(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> sourceIds
    ) {
        List<JoinMethodSpec> methods = new ArrayList<>();
        for (String sourceId : sourceIds) {
            NodeInfo source = nodes.get(sourceId);
            if (source == null || (source.type != NodeType.TASK && source.type != NodeType.CALL_ACTIVITY)) {
                continue;
            }
            methods.add(new JoinMethodSpec(
                    "on" + BpmnScaffolder.toClassName(source.name),
                    processId + "_" + source.name + "_output",
                    source.name
            ));
        }
        return methods;
    }

    private static String buildJoinCondition(List<JoinMethodSpec> incomingMethods) {
        if (incomingMethods.isEmpty()) {
            return "false";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < incomingMethods.size(); i++) {
            if (i > 0) {
                builder.append(" && ");
            }
            JoinMethodSpec method = incomingMethods.get(i);
            builder.append("tokens.stream().anyMatch(token -> \"")
                    .append(method.activityId)
                    .append("\".equals(token.at()))");
        }
        return builder.toString();
    }

    private static String buildJoinMethodsBlock(List<JoinMethodSpec> incomingMethods) {
        StringBuilder builder = new StringBuilder();
        for (JoinMethodSpec method : incomingMethods) {
            builder.append("    @Incoming(\"")
                    .append(method.channel)
                    .append("\")\n")
                    .append("    public CompletionStage<Void> ")
                    .append(method.method)
                    .append("(Message<String> msg) {\n")
                    .append("        return handleJoin(\"")
                    .append(method.activityId)
                    .append("\", msg);\n")
                    .append("    }\n\n");
        }
        return builder.toString();
    }

    private static String resolveEndActivityId(Map<String, NodeInfo> nodes, NodeInfo node) {
        if (node.outgoingIds.isEmpty()) {
            return "completed";
        }
        List<String> endTargets = new ArrayList<>();
        for (String targetId : node.outgoingIds) {
            NodeInfo target = nodes.get(targetId);
            if (target == null) {
                return null;
            }
            if (target.type != NodeType.END) {
                return null;
            }
            endTargets.add(target.name);
        }
        return endTargets.getFirst();
    }
}
