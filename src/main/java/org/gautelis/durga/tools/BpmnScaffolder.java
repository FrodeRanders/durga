package org.gautelis.durga.tools;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent;
import org.camunda.bpm.model.bpmn.instance.CallActivity;
import org.camunda.bpm.model.bpmn.instance.ConditionExpression;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.EscalationEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ExclusiveGateway;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.ManualTask;
import org.camunda.bpm.model.bpmn.instance.IntermediateCatchEvent;
import org.camunda.bpm.model.bpmn.instance.IntermediateThrowEvent;
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ParallelGateway;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.camunda.bpm.model.bpmn.instance.SequenceFlow;
import org.camunda.bpm.model.bpmn.instance.ServiceTask;
import org.camunda.bpm.model.bpmn.instance.SignalEventDefinition;
import org.camunda.bpm.model.bpmn.instance.StartEvent;
import org.camunda.bpm.model.bpmn.instance.SubProcess;
import org.camunda.bpm.model.bpmn.instance.Task;
import org.camunda.bpm.model.bpmn.instance.TimeCycle;
import org.camunda.bpm.model.bpmn.instance.TimeDate;
import org.camunda.bpm.model.bpmn.instance.TimeDuration;
import org.camunda.bpm.model.bpmn.instance.TimerEventDefinition;
import org.camunda.bpm.model.bpmn.instance.UserTask;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Scaffolds a BPMN model into an explicit Durga/Kafka runtime project.
 * <p>
 * The generator favors inspectable source code and helper scripts over a hidden workflow engine,
 * so the generated project can be studied, modified, and demoed locally.
 */
public class BpmnScaffolder {
    /**
     * Generates a project from a BPMN file.
     *
     * @param args command-line arguments such as {@code --dry-run}, {@code --output-dir}, and the
     *             BPMN file path
     */
    public static void main(String[] args) {
        ParsedArgs parsed = parseArgs(args);
        if (parsed == null) {
            System.exit(1);
        }

        File bpmnFile = new File(parsed.bpmnPath);
        if (!bpmnFile.exists()) {
            System.err.println("BPMN file not found: " + bpmnFile.getAbsolutePath());
            System.exit(1);
        }

        Path outputRoot = Path.of(parsed.outputDir);
        Path javaOutput = outputRoot.resolve("src/main/java/se/fk/kafka/generated");
        Path coreJavaOutput = outputRoot.resolve("src/main/java/se/fk/kafka");
        Path mainSourceRoot = Path.of("src/main/java");

        BpmnModelInstance model = Bpmn.readModelFromFile(bpmnFile);
        Process process = model.getModelElementsByType(Process.class).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No <process> element found."));

        String processId = normalize(process.getId() != null ? process.getId() : "process");
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        Map<String, List<FlowInfo>> flowsBySource = new LinkedHashMap<>();
        List<EventSubProcessSpec> eventSubProcessSpecs = collectEventSubProcessSpecs(model, nodes);
        List<SubProcessSpec> subProcessSpecs = collectSubProcessSpecs(model, nodes);
        List<String> subProcesses = subProcessSpecs.stream().map(spec -> spec.name).toList();
        List<TaskSpec> taskSpecs = collectTaskSpecs(model, nodes);
        List<String> tasks = taskSpecs.stream().map(task -> task.name).toList();
        List<TimerSpec> timerSpecs = collectTimerSpecs(model, nodes);
        List<String> timers = timerSpecs.stream().map(timer -> timer.name).toList();
        List<BoundaryTimerSpec> boundaryTimerSpecs = collectBoundaryTimerSpecs(model, nodes);
        List<String> boundaryTimers = boundaryTimerSpecs.stream().map(timer -> timer.name).toList();
        List<BoundaryErrorSpec> boundaryErrorSpecs = collectBoundaryErrorSpecs(model, nodes);
        List<String> boundaryErrors = boundaryErrorSpecs.stream().map(error -> error.name).toList();
        List<BoundaryEscalationSpec> boundaryEscalationSpecs = collectBoundaryEscalationSpecs(model, nodes);
        List<String> boundaryEscalations = boundaryEscalationSpecs.stream().map(escalation -> escalation.name).toList();
        List<MessageCatchSpec> messageCatchSpecs = collectMessageCatchSpecs(model, nodes);
        List<MessageThrowSpec> messageThrowSpecs = collectMessageThrowSpecs(model, nodes);
        List<String> messageEvents = collectMessageEvents(messageCatchSpecs, messageThrowSpecs, eventSubProcessSpecs);
        List<String> messageTopics = collectMessageTopics(messageCatchSpecs, messageThrowSpecs, eventSubProcessSpecs);
        List<SignalCatchSpec> signalCatchSpecs = collectSignalCatchSpecs(model, nodes);
        List<SignalThrowSpec> signalThrowSpecs = collectSignalThrowSpecs(model, nodes);
        List<String> signalEvents = collectSignalEvents(signalCatchSpecs, signalThrowSpecs, eventSubProcessSpecs);
        List<String> signalTopics = collectSignalTopics(signalCatchSpecs, signalThrowSpecs, eventSubProcessSpecs);
        List<CallActivitySpec> callActivitySpecs = collectCallActivitySpecs(model, nodes);
        List<String> callActivities = callActivitySpecs.stream().map(call -> call.name).toList();

        List<NodeInfo> xors = new ArrayList<>();
        for (ExclusiveGateway gateway : model.getModelElementsByType(ExclusiveGateway.class)) {
            NodeInfo info = new NodeInfo(gateway.getId(), normalize(nameOrId(gateway.getName(), gateway.getId())),
                    NodeType.XOR);
            if (gateway.getDefault() != null) {
                info.defaultFlowId = gateway.getDefault().getId();
            }
            nodes.put(gateway.getId(), info);
            xors.add(info);
        }

        List<NodeInfo> ands = new ArrayList<>();
        for (ParallelGateway gateway : model.getModelElementsByType(ParallelGateway.class)) {
            NodeInfo info = new NodeInfo(gateway.getId(), normalize(nameOrId(gateway.getName(), gateway.getId())),
                    NodeType.AND);
            nodes.put(gateway.getId(), info);
            ands.add(info);
        }

        for (StartEvent startEvent : model.getModelElementsByType(StartEvent.class)) {
            if (enclosingSubProcessId(startEvent) != null) {
                continue;
            }
            nodes.put(startEvent.getId(), new NodeInfo(startEvent.getId(), "start", NodeType.START));
        }

        for (EndEvent endEvent : model.getModelElementsByType(EndEvent.class)) {
            if (enclosingSubProcessId(endEvent) != null) {
                continue;
            }
            nodes.put(endEvent.getId(), new NodeInfo(
                    endEvent.getId(),
                    normalize(nameOrId(endEvent.getName(), endEvent.getId())),
                    NodeType.END
            ));
        }

        for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
            FlowNode source = flow.getSource();
            FlowNode target = flow.getTarget();
            NodeInfo sourceInfo = nodes.get(source.getId());
            NodeInfo targetInfo = nodes.get(target.getId());
            if (sourceInfo == null || targetInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(flowInfo(flow));
        }

        // Load every template group up front so the rest of main can stay focused on
        // "what gets generated" rather than on ST4 plumbing.
        STGroupString group = new STGroupString(loadTemplates());
        if (!parsed.dryRun) {
            try {
                Files.createDirectories(javaOutput);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create output directories", e);
            }
        }

        Set<String> existingSources = indexSourceFiles(mainSourceRoot);
        List<String> generatedFiles = new ArrayList<>();

        // The generation order is intentional: shared runtime support first, then BPMN handlers,
        // and finally scripts/config/readme assets that describe the generated topology.
        generateCoreClasses(group, coreJavaOutput, outputRoot, generatedFiles, parsed.dryRun);

        TaskRoutingGenerator.generateTaskHandlers(
                group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, parsed.transactions,
                processId, taskSpecs, existingSources
        );

        generateStarter(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateDemoRunner(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, tasks);
        generateTaskInputPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskCompletionPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskFailurePublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskEscalationPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateCallActivityCompletionPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        EventTemplateGenerator.generateTimerHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, timerSpecs);
        EventTemplateGenerator.generateBoundaryTimerHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, boundaryTimerSpecs);
        EventTemplateGenerator.generateBoundaryErrorHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, boundaryErrorSpecs);
        EventTemplateGenerator.generateBoundaryEscalationHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, boundaryEscalationSpecs);
        EventTemplateGenerator.generateMessageCatchHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, messageCatchSpecs);
        EventTemplateGenerator.generateMessageThrowHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, messageThrowSpecs);
        EventTemplateGenerator.generateSignalCatchHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, signalCatchSpecs);
        EventTemplateGenerator.generateSignalThrowHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, signalThrowSpecs);
        EventTemplateGenerator.generateCallActivityHandlers(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, callActivitySpecs);
        SubProcessTemplateGenerator.generateSubProcessHandlers(
                group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, subProcessSpecs
        );
        SubProcessTemplateGenerator.generateEventSubProcessHandlers(
                group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId, nodes, eventSubProcessSpecs
        );
        EventTemplateGenerator.generateMessageEventPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        EventTemplateGenerator.generateSignalEventPublisher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateProcessEventsWatcher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskOutputWatcher(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);

        TaskRoutingGenerator.generateGateways(
                processId, group, javaOutput, nodes, flowsBySource, xors, ands, existingSources,
                outputRoot, generatedFiles, parsed.dryRun
        );
        TaskRoutingGenerator.generateOrchestrator(
                processId, group, javaOutput, nodes, existingSources, outputRoot, generatedFiles, parsed.dryRun
        );

        List<String> allTimers = combineNames(timers, boundaryTimers);
        List<String> externalTopics = combineNames(messageTopics, signalTopics);
        String yamlPreview = renderYamlPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses);
        String topicsPreview = renderTopicsPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses);
        String pomPreview = renderPomPreview(group, processId);
        String runLocalPreview = renderRunLocalPreview(group, processId);
        String demoPreview = renderDemoPreview(group, processId, tasks);
        String taskInputPreview = renderTaskInputPreview(group, processId);
        String taskCompletionPreview = renderTaskCompletionPreview(group, processId);
        String taskFailurePreview = renderTaskFailurePreview(group, processId);
        String taskEscalationPreview = renderTaskEscalationPreview(group, processId);
        String callActivityCompletionPreview = renderCallActivityCompletionPreview(group, processId);
        String messageEventPreview = renderMessageEventPreview(group, processId);
        String signalEventPreview = renderSignalEventPreview(group, processId);
        String processEventsWatchPreview = renderProcessEventsWatchPreview(group, processId);
        String taskOutputWatchPreview = renderTaskOutputWatchPreview(group, processId);
        String payloadPreview = GeneratedProjectSupport.renderTaskPayloadsPreview(processId, tasks);
        if (!parsed.dryRun) {
            // application.yml is merged rather than replaced so the generated project can keep
            // accumulating channel config across repeated scaffold runs.
            GeneratedProjectSupport.mergeApplicationYaml(
                    processId, tasks, allTimers, externalTopics, callActivities, subProcesses, outputRoot
            );
            Path topicsPath = outputRoot.resolve("topics.sh");
            writeFile(topicsPath, topicsPreview);
            generatedFiles.add(outputRoot.relativize(topicsPath).toString());

            Path pomPath = outputRoot.resolve("pom.xml");
            writeFile(pomPath, pomPreview);
            generatedFiles.add(outputRoot.relativize(pomPath).toString());

            Path runLocalPath = outputRoot.resolve("run-local.sh");
            writeFile(runLocalPath, runLocalPreview);
            generatedFiles.add(outputRoot.relativize(runLocalPath).toString());

            Path demoPath = outputRoot.resolve("demo-scenario.sh");
            writeFile(demoPath, demoPreview);
            generatedFiles.add(outputRoot.relativize(demoPath).toString());

            Path taskInputPath = outputRoot.resolve("send-task-input.sh");
            writeFile(taskInputPath, taskInputPreview);
            generatedFiles.add(outputRoot.relativize(taskInputPath).toString());

            Path taskCompletionPath = outputRoot.resolve("complete-task.sh");
            writeFile(taskCompletionPath, taskCompletionPreview);
            generatedFiles.add(outputRoot.relativize(taskCompletionPath).toString());

            Path taskFailurePath = outputRoot.resolve("fail-task.sh");
            writeFile(taskFailurePath, taskFailurePreview);
            generatedFiles.add(outputRoot.relativize(taskFailurePath).toString());

            Path taskEscalationPath = outputRoot.resolve("escalate-task.sh");
            writeFile(taskEscalationPath, taskEscalationPreview);
            generatedFiles.add(outputRoot.relativize(taskEscalationPath).toString());

            Path callActivityCompletionPath = outputRoot.resolve("complete-call-activity.sh");
            writeFile(callActivityCompletionPath, callActivityCompletionPreview);
            generatedFiles.add(outputRoot.relativize(callActivityCompletionPath).toString());

            Path messageEventPath = outputRoot.resolve("send-message-event.sh");
            writeFile(messageEventPath, messageEventPreview);
            generatedFiles.add(outputRoot.relativize(messageEventPath).toString());

            Path signalEventPath = outputRoot.resolve("send-signal-event.sh");
            writeFile(signalEventPath, signalEventPreview);
            generatedFiles.add(outputRoot.relativize(signalEventPath).toString());

            Path processEventsWatchPath = outputRoot.resolve("watch-process-events.sh");
            writeFile(processEventsWatchPath, processEventsWatchPreview);
            generatedFiles.add(outputRoot.relativize(processEventsWatchPath).toString());

            Path taskOutputWatchPath = outputRoot.resolve("watch-task-output.sh");
            writeFile(taskOutputWatchPath, taskOutputWatchPreview);
            generatedFiles.add(outputRoot.relativize(taskOutputWatchPath).toString());

            Path payloadPath = outputRoot.resolve("task-payloads.json");
            writeFile(payloadPath, payloadPreview);
            generatedFiles.add(outputRoot.relativize(payloadPath).toString());
        }

        List<String> boundaryEvents = combineNames(combineNames(boundaryTimers, boundaryErrors), boundaryEscalations);
        Map<String, Object> summary = GeneratedProjectSupport.buildSummary(
                processId, tasks, allTimers, messageEvents, messageTopics, signalEvents, signalTopics,
                boundaryEvents, callActivities, subProcesses, xors, ands, generatedFiles
        );
        String summaryJson = GeneratedProjectSupport.renderSummaryJson(summary);

        if (!parsed.dryRun) {
            GeneratedProjectSupport.writeSummary(outputRoot, summaryJson);
            GeneratedProjectSupport.writeGeneratedReadme(
                    outputRoot, processId, tasks, allTimers, messageEvents, messageTopics, signalEvents,
                    signalTopics, boundaryEvents, callActivities, subProcesses, xors, ands, generatedFiles,
                    payloadPreview
            );
        }

        System.out.println("Generated in " + outputRoot.toAbsolutePath());
        System.out.println("Process: " + processId);
        System.out.println("Tasks: " + tasks);
        System.out.println("Call activities: " + callActivities);
        System.out.println("Embedded subprocesses: " + subProcesses);
        System.out.println("Event subprocesses: " + eventSubProcessSpecs.stream().map(spec -> spec.name).toList());
        System.out.println("Timers: " + allTimers);
        System.out.println("Boundary events: " + boundaryEvents);
        System.out.println("Message events: " + messageEvents);
        System.out.println("Signal events: " + signalEvents);
        System.out.println("XOR gateways: " + xors.stream().map(info -> info.name).toList());
        System.out.println("AND gateways: " + ands.stream().map(info -> info.name).toList());

        if (parsed.dryRun) {
            System.out.println();
            System.out.println("Dry run preview: summary.json");
            System.out.println(summaryJson);
            System.out.println();
            System.out.println("Dry run preview: application.yml additions");
            System.out.println(yamlPreview);
            System.out.println();
            System.out.println("Dry run preview: topics.sh");
            System.out.println(topicsPreview);
            System.out.println();
            System.out.println("Dry run preview: pom.xml");
            System.out.println(pomPreview);
            System.out.println();
            System.out.println("Dry run preview: run-local.sh");
            System.out.println(runLocalPreview);
            System.out.println();
            System.out.println("Dry run preview: demo-scenario.sh");
            System.out.println(demoPreview);
            System.out.println();
            System.out.println("Dry run preview: send-task-input.sh");
            System.out.println(taskInputPreview);
            System.out.println();
            System.out.println("Dry run preview: complete-task.sh");
            System.out.println(taskCompletionPreview);
            System.out.println();
            System.out.println("Dry run preview: fail-task.sh");
            System.out.println(taskFailurePreview);
            System.out.println();
            System.out.println("Dry run preview: escalate-task.sh");
            System.out.println(taskEscalationPreview);
            System.out.println();
            System.out.println("Dry run preview: complete-call-activity.sh");
            System.out.println(callActivityCompletionPreview);
            System.out.println();
            System.out.println("Dry run preview: send-message-event.sh");
            System.out.println(messageEventPreview);
            System.out.println();
            System.out.println("Dry run preview: send-signal-event.sh");
            System.out.println(signalEventPreview);
            System.out.println();
            System.out.println("Dry run preview: watch-process-events.sh");
            System.out.println(processEventsWatchPreview);
            System.out.println();
            System.out.println("Dry run preview: watch-task-output.sh");
            System.out.println(taskOutputWatchPreview);
            System.out.println();
            System.out.println("Dry run preview: task-payloads.json");
            System.out.println(payloadPreview);
        }
    }

    private static ParsedArgs parseArgs(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: BpmnScaffolder <path-to-bpmn.xml> [output-dir] [--out <dir>] [--dry-run] [--transactions]");
            return null;
        }
        boolean dryRun = false;
        boolean transactions = false;
        String outputDir = null;
        List<String> positional = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--dry-run".equals(arg)) {
                dryRun = true;
            } else if ("--transactions".equals(arg)) {
                transactions = true;
            } else if (arg.startsWith("--out=")) {
                outputDir = arg.substring("--out=".length());
            } else if ("--out".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --out");
                    return null;
                }
                outputDir = args[++i];
            } else {
                positional.add(arg);
            }
        }

        if (positional.isEmpty()) {
            System.err.println("Usage: BpmnScaffolder <path-to-bpmn.xml> [output-dir] [--out <dir>] [--dry-run] [--transactions]");
            return null;
        }

        String bpmnPath = positional.get(0);
        if (outputDir == null) {
            outputDir = positional.size() > 1 ? positional.get(1) : "generated";
        }
        return new ParsedArgs(bpmnPath, outputDir, dryRun, transactions);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unnamed";
        }
        String normalized = value.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+", "")
                .replaceAll("_+$", "");
        return normalized.isBlank() ? "unnamed" : normalized;
    }

    private static String nameOrId(String name, String id) {
        if (name != null && !name.isBlank()) {
            return name;
        }
        if (id != null && !id.isBlank()) {
            return id;
        }
        return "unnamed";
    }

    static Optional<String> resolveInputChannel(
            String processId,
            Map<String, NodeInfo> nodes,
            String sourceId
    ) {
        NodeInfo source = nodes.get(sourceId);
        if (source == null) {
            return Optional.empty();
        }
        if (source.type == NodeType.TASK) {
            return Optional.of(processId + "_" + source.name + "_output");
        }
        if (source.type == NodeType.CALL_ACTIVITY) {
            return Optional.of(processId + "_" + source.name + "_output");
        }
        if (source.type == NodeType.SUB_PROCESS) {
            return Optional.of(processId + "_" + source.name + "_output");
        }
        if (source.type == NodeType.TIMER) {
            return Optional.of(processId + "_" + source.name + "_input");
        }
        if (source.type == NodeType.MESSAGE_THROW) {
            return Optional.of(processId + "_" + source.name + "_input");
        }
        if (source.type == NodeType.SIGNAL_THROW) {
            return Optional.of(processId + "_" + source.name + "_input");
        }
        if (source.type == NodeType.START) {
            return Optional.of(processId + "_start");
        }
        return Optional.empty();
    }

    static List<OutputSpec> resolveOutputChannels(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> targetIds,
            List<FlowInfo> flows,
            String defaultFlowId
    ) {
        List<OutputSpec> outputs = new ArrayList<>();
        for (String targetId : targetIds) {
            NodeInfo target = nodes.get(targetId);
            if (target == null) {
                continue;
            }
            if (target.type != NodeType.TASK && target.type != NodeType.CALL_ACTIVITY && target.type != NodeType.SUB_PROCESS && target.type != NodeType.TIMER
                    && target.type != NodeType.MESSAGE_CATCH && target.type != NodeType.MESSAGE_THROW
                    && target.type != NodeType.SIGNAL_CATCH && target.type != NodeType.SIGNAL_THROW) {
                continue;
            }
            FlowInfo flowInfo = findFlowInfo(flows, target.id);
            String condition = flowInfo != null ? flowInfo.condition : null;
            boolean isDefault = flowInfo != null && flowInfo.id.equals(defaultFlowId);
            outputs.add(new OutputSpec(
                    "task" + toClassName(target.name) + "Emitter",
                    processId + "_" + target.name + "_input",
                    target.name,
                    target.type,
                    condition,
                    isDefault,
                    null
            ));
        }
        return outputs;
    }

    static Optional<OutputSpec> resolveSingleOutput(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> targetIds
    ) {
        if (targetIds.isEmpty()) {
            return Optional.empty();
        }
        NodeInfo target = nodes.get(targetIds.getFirst());
        if (target == null) {
            return Optional.empty();
        }
        if (target.type != NodeType.TASK && target.type != NodeType.CALL_ACTIVITY && target.type != NodeType.SUB_PROCESS && target.type != NodeType.TIMER
                && target.type != NodeType.END && target.type != NodeType.MESSAGE_CATCH && target.type != NodeType.MESSAGE_THROW
                && target.type != NodeType.SIGNAL_CATCH && target.type != NodeType.SIGNAL_THROW) {
            return Optional.empty();
        }
        String channel = target.type == NodeType.END ? "process-events" : processId + "_" + target.name + "_input";
        return Optional.of(new OutputSpec(
                "task" + toClassName(target.name) + "Emitter",
                channel,
                target.name,
                target.type,
                null,
                false,
                null
        ));
    }

    private static List<TaskSpec> collectTaskSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        Map<String, TaskSpec> taskSpecs = new LinkedHashMap<>();
        registerTaskSpecs(model.getModelElementsByType(ServiceTask.class), TaskKind.SERVICE, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(UserTask.class), TaskKind.USER, taskSpecs, nodes);
        registerTaskSpecs(model.getModelElementsByType(ManualTask.class), TaskKind.MANUAL, taskSpecs, nodes);
        for (CallActivity callActivity : model.getModelElementsByType(CallActivity.class)) {
            String name = normalize(nameOrId(callActivity.getName(), callActivity.getId()));
            taskSpecs.put(callActivity.getId(), new TaskSpec(callActivity.getId(), name, TaskKind.CALL));
        }
        for (Task task : model.getModelElementsByType(Task.class)) {
            if (taskSpecs.containsKey(task.getId())) {
                continue;
            }
            String name = normalize(nameOrId(task.getName(), task.getId()));
            taskSpecs.put(task.getId(), new TaskSpec(task.getId(), name, TaskKind.GENERIC));
            nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, TaskKind.GENERIC));
        }
        return new ArrayList<>(taskSpecs.values());
    }

    private static List<TimerSpec> collectTimerSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<TimerSpec> timers = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            TimerEventDefinition timerDefinition = event.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (timerDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            TimerSpec timerSpec = timerSpec(event.getId(), name, timerDefinition);
            timers.add(timerSpec);
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.TIMER));
        }
        return timers;
    }

    private static List<EventSubProcessSpec> collectEventSubProcessSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<EventSubProcessSpec> specs = new ArrayList<>();
        for (SubProcess subProcess : model.getModelElementsByType(SubProcess.class)) {
            if (!isEventSubProcess(subProcess)) {
                continue;
            }
            String name = normalize(nameOrId(subProcess.getName(), subProcess.getId()));
            StartEvent triggerStart = subProcess.getChildElementsByType(StartEvent.class).stream().findFirst().orElse(null);
            if (triggerStart == null) {
                continue;
            }

            MessageEventDefinition messageDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            SignalEventDefinition signalDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            TimerEventDefinition timerDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            ErrorEventDefinition errorDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(ErrorEventDefinition.class::isInstance)
                    .map(ErrorEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            EscalationEventDefinition escalationDefinition = triggerStart.getEventDefinitions().stream()
                    .filter(EscalationEventDefinition.class::isInstance)
                    .map(EscalationEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null && signalDefinition == null && timerDefinition == null
                    && errorDefinition == null && escalationDefinition == null) {
                continue;
            }

            EventTriggerKind triggerKind;
            String triggerName;
            String triggerTopic;
            String timerType = null;
            String timerExpression = null;
            if (messageDefinition != null) {
                triggerKind = EventTriggerKind.MESSAGE;
                triggerName = messageName(name, messageDefinition);
                triggerTopic = processMessageTopic(model, triggerName);
            } else if (signalDefinition != null) {
                triggerKind = EventTriggerKind.SIGNAL;
                triggerName = signalName(name, signalDefinition);
                triggerTopic = processSignalTopic(model, triggerName);
            } else if (errorDefinition != null) {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.ERROR;
                triggerName = errorDefinition.getError() != null && errorDefinition.getError().getErrorCode() != null
                        ? normalize(errorDefinition.getError().getErrorCode())
                        : name + "_error";
                triggerTopic = null;
            } else if (escalationDefinition != null) {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.ESCALATION;
                triggerName = escalationDefinition.getEscalation() != null
                        && escalationDefinition.getEscalation().getEscalationCode() != null
                        ? normalize(escalationDefinition.getEscalation().getEscalationCode())
                        : name + "_escalation";
                triggerTopic = null;
            } else {
                String enclosingScopeId = enclosingSubProcessId(subProcess);
                if (enclosingScopeId == null) {
                    continue;
                }
                triggerKind = EventTriggerKind.TIMER;
                TimerSpec timerSpec = timerSpec(triggerStart.getId(), name, timerDefinition);
                triggerName = timerSpec.name;
                triggerTopic = null;
                timerType = timerSpec.timerType;
                timerExpression = timerSpec.expression;
            }

            List<String> entryTargetIds = new ArrayList<>();
            List<String> exitSourceIds = new ArrayList<>();
            for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
                FlowNode source = flow.getSource();
                FlowNode target = flow.getTarget();
                if (source instanceof StartEvent && subProcess.getId().equals(enclosingSubProcessId(source))) {
                    entryTargetIds.add(target.getId());
                }
                if (target instanceof EndEvent && subProcess.getId().equals(enclosingSubProcessId(target))) {
                    exitSourceIds.add(source.getId());
                }
            }

            List<String> scopeActivityIds = new ArrayList<>();
            for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
                if (!isWithinSubProcess(flowNode, subProcess.getId())) {
                    continue;
                }
                if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                    continue;
                }
                scopeActivityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
            }
            String enclosingScopeId = enclosingSubProcessId(subProcess);
            List<String> cancellationScopeActivityIds = collectEventSubProcessCancellationScopeActivityIds(
                    model,
                    subProcess.getId(),
                    enclosingScopeId
            );

            specs.add(new EventSubProcessSpec(
                    subProcess.getId(),
                    name,
                    triggerKind,
                    triggerName,
                    triggerTopic,
                    timerType,
                    timerExpression,
                    enclosingScopeId != null ? normalize(nameOrId(
                            ((SubProcess) model.getModelElementById(enclosingScopeId)).getName(),
                            enclosingScopeId
                    )) : null,
                    isInterruptingStart(triggerStart),
                    cancellationScopeActivityIds,
                    distinct(entryTargetIds),
                    distinct(exitSourceIds),
                    distinct(scopeActivityIds)
            ));
        }
        return specs;
    }

    private static List<String> collectEventSubProcessCancellationScopeActivityIds(
            BpmnModelInstance model,
            String eventSubProcessId,
            String enclosingScopeId
    ) {
        List<String> activityIds = new ArrayList<>();
        for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
            if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                continue;
            }
            if (isWithinSubProcess(flowNode, eventSubProcessId)) {
                continue;
            }
            String flowNodeEnclosingScope = enclosingSubProcessId(flowNode);
            if (enclosingScopeId == null) {
                if (flowNodeEnclosingScope != null) {
                    continue;
                }
            } else if (!enclosingScopeId.equals(flowNodeEnclosingScope)) {
                continue;
            }
            activityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
        }
        if (enclosingScopeId != null) {
            ModelElementInstance enclosingScope = model.getModelElementById(enclosingScopeId);
            if (enclosingScope instanceof SubProcess subProcess) {
                activityIds.add(normalize(nameOrId(subProcess.getName(), subProcess.getId())));
            }
        }
        return distinct(activityIds);
    }

    private static List<SubProcessSpec> collectSubProcessSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SubProcessSpec> specs = new ArrayList<>();
        for (SubProcess subProcess : model.getModelElementsByType(SubProcess.class)) {
            if (isEventSubProcess(subProcess)) {
                continue;
            }
            String name = normalize(nameOrId(subProcess.getName(), subProcess.getId()));
            nodes.put(subProcess.getId(), new NodeInfo(subProcess.getId(), name, NodeType.SUB_PROCESS));
            List<String> entryTargetIds = new ArrayList<>();
            List<String> exitSourceIds = new ArrayList<>();
            for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
                FlowNode source = flow.getSource();
                FlowNode target = flow.getTarget();
                if (source instanceof StartEvent && subProcess.getId().equals(enclosingSubProcessId(source))) {
                    entryTargetIds.add(target.getId());
                }
                if (target instanceof EndEvent && subProcess.getId().equals(enclosingSubProcessId(target))) {
                    exitSourceIds.add(source.getId());
                }
            }
            List<String> scopeActivityIds = new ArrayList<>();
            for (FlowNode flowNode : model.getModelElementsByType(FlowNode.class)) {
                if (!isWithinSubProcess(flowNode, subProcess.getId())) {
                    continue;
                }
                if (flowNode instanceof StartEvent || flowNode instanceof EndEvent) {
                    continue;
                }
                scopeActivityIds.add(normalize(nameOrId(flowNode.getName(), flowNode.getId())));
            }
            specs.add(new SubProcessSpec(subProcess.getId(), name, distinct(entryTargetIds), distinct(exitSourceIds), distinct(scopeActivityIds)));
        }
        return specs;
    }

    private static List<MessageCatchSpec> collectMessageCatchSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<MessageCatchSpec> specs = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            MessageEventDefinition messageDefinition = event.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String messageName = messageName(name, messageDefinition);
            specs.add(new MessageCatchSpec(event.getId(), name, messageName, processMessageTopic(model, messageName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.MESSAGE_CATCH));
        }
        return specs;
    }

    private static List<CallActivitySpec> collectCallActivitySpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<CallActivitySpec> specs = new ArrayList<>();
        for (CallActivity callActivity : model.getModelElementsByType(CallActivity.class)) {
            String name = normalize(nameOrId(callActivity.getName(), callActivity.getId()));
            String calledElement = callActivity.getCalledElement() != null && !callActivity.getCalledElement().isBlank()
                    ? normalize(callActivity.getCalledElement())
                    : name + "_called_process";
            specs.add(new CallActivitySpec(callActivity.getId(), name, calledElement));
            nodes.put(callActivity.getId(), new NodeInfo(callActivity.getId(), name, NodeType.CALL_ACTIVITY, TaskKind.CALL));
        }
        return specs;
    }

    private static List<MessageThrowSpec> collectMessageThrowSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<MessageThrowSpec> specs = new ArrayList<>();
        for (IntermediateThrowEvent event : model.getModelElementsByType(IntermediateThrowEvent.class)) {
            MessageEventDefinition messageDefinition = event.getEventDefinitions().stream()
                    .filter(MessageEventDefinition.class::isInstance)
                    .map(MessageEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (messageDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String messageName = messageName(name, messageDefinition);
            specs.add(new MessageThrowSpec(event.getId(), name, messageName, processMessageTopic(model, messageName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.MESSAGE_THROW));
        }
        return specs;
    }

    private static List<SignalCatchSpec> collectSignalCatchSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SignalCatchSpec> specs = new ArrayList<>();
        for (IntermediateCatchEvent event : model.getModelElementsByType(IntermediateCatchEvent.class)) {
            SignalEventDefinition signalDefinition = event.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (signalDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String signalName = signalName(name, signalDefinition);
            specs.add(new SignalCatchSpec(event.getId(), name, signalName, processSignalTopic(model, signalName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.SIGNAL_CATCH));
        }
        return specs;
    }

    private static List<SignalThrowSpec> collectSignalThrowSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<SignalThrowSpec> specs = new ArrayList<>();
        for (IntermediateThrowEvent event : model.getModelElementsByType(IntermediateThrowEvent.class)) {
            SignalEventDefinition signalDefinition = event.getEventDefinitions().stream()
                    .filter(SignalEventDefinition.class::isInstance)
                    .map(SignalEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (signalDefinition == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String signalName = signalName(name, signalDefinition);
            specs.add(new SignalThrowSpec(event.getId(), name, signalName, processSignalTopic(model, signalName)));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.SIGNAL_THROW));
        }
        return specs;
    }

    private static List<String> collectMessageEvents(
            List<MessageCatchSpec> catches,
            List<MessageThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> events = new ArrayList<>();
        catches.forEach(spec -> events.add(spec.name));
        for (MessageThrowSpec spec : throwsEvents) {
            if (!events.contains(spec.name)) {
                events.add(spec.name);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.MESSAGE && !events.contains(spec.triggerName)) {
                events.add(spec.triggerName);
            }
        }
        return events;
    }

    private static List<String> collectMessageTopics(
            List<MessageCatchSpec> catches,
            List<MessageThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> topics = new ArrayList<>();
        for (MessageCatchSpec spec : catches) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (MessageThrowSpec spec : throwsEvents) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.MESSAGE && !topics.contains(spec.triggerTopic)) {
                topics.add(spec.triggerTopic);
            }
        }
        return topics;
    }

    private static List<String> collectSignalEvents(
            List<SignalCatchSpec> catches,
            List<SignalThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> events = new ArrayList<>();
        catches.forEach(spec -> events.add(spec.name));
        for (SignalThrowSpec spec : throwsEvents) {
            if (!events.contains(spec.name)) {
                events.add(spec.name);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.SIGNAL && !events.contains(spec.triggerName)) {
                events.add(spec.triggerName);
            }
        }
        return events;
    }

    private static List<String> collectSignalTopics(
            List<SignalCatchSpec> catches,
            List<SignalThrowSpec> throwsEvents,
            List<EventSubProcessSpec> eventSubProcesses
    ) {
        List<String> topics = new ArrayList<>();
        for (SignalCatchSpec spec : catches) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (SignalThrowSpec spec : throwsEvents) {
            if (!topics.contains(spec.topic)) {
                topics.add(spec.topic);
            }
        }
        for (EventSubProcessSpec spec : eventSubProcesses) {
            if (spec.triggerKind == EventTriggerKind.SIGNAL && !topics.contains(spec.triggerTopic)) {
                topics.add(spec.triggerTopic);
            }
        }
        return topics;
    }

    private static List<String> combineNames(List<String> primary, List<String> secondary) {
        List<String> combined = new ArrayList<>(primary);
        for (String value : secondary) {
            if (!combined.contains(value)) {
                combined.add(value);
            }
        }
        return combined;
    }

    private static String messageName(String fallbackName, MessageEventDefinition definition) {
        if (definition.getMessage() != null && definition.getMessage().getName() != null && !definition.getMessage().getName().isBlank()) {
            return normalize(definition.getMessage().getName());
        }
        return fallbackName;
    }

    private static String processMessageTopic(BpmnModelInstance model, String messageName) {
        Process process = model.getModelElementsByType(Process.class).stream().findFirst().orElse(null);
        String processId = process != null ? normalize(process.getId()) : "process";
        return processId + "_" + messageName + "_message";
    }

    private static String signalName(String fallbackName, SignalEventDefinition definition) {
        if (definition.getSignal() != null && definition.getSignal().getName() != null && !definition.getSignal().getName().isBlank()) {
            return normalize(definition.getSignal().getName());
        }
        return fallbackName;
    }

    private static String processSignalTopic(BpmnModelInstance model, String signalName) {
        Process process = model.getModelElementsByType(Process.class).stream().findFirst().orElse(null);
        String processId = process != null ? normalize(process.getId()) : "process";
        return processId + "_" + signalName + "_signal";
    }

    private static List<BoundaryTimerSpec> collectBoundaryTimerSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryTimerSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            TimerEventDefinition timerDefinition = event.getEventDefinitions().stream()
                    .filter(TimerEventDefinition.class::isInstance)
                    .map(TimerEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (timerDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            TimerSpec timerSpec = timerSpec(event.getId(), name, timerDefinition);
            specs.add(new BoundaryTimerSpec(
                    event.getId(),
                    name,
                    timerSpec.timerType,
                    timerSpec.expression,
                    attachedActivityId,
                    event.cancelActivity()
            ));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_TIMER));
        }
        return specs;
    }

    private static List<BoundaryErrorSpec> collectBoundaryErrorSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryErrorSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            ErrorEventDefinition errorDefinition = event.getEventDefinitions().stream()
                    .filter(ErrorEventDefinition.class::isInstance)
                    .map(ErrorEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (errorDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            String errorCode = errorDefinition.getError() != null && errorDefinition.getError().getErrorCode() != null
                    ? normalize(errorDefinition.getError().getErrorCode())
                    : name + "_error";
            specs.add(new BoundaryErrorSpec(event.getId(), name, attachedActivityId, errorCode, event.cancelActivity()));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_ERROR));
        }
        return specs;
    }

    private static List<BoundaryEscalationSpec> collectBoundaryEscalationSpecs(BpmnModelInstance model, Map<String, NodeInfo> nodes) {
        List<BoundaryEscalationSpec> specs = new ArrayList<>();
        for (BoundaryEvent event : model.getModelElementsByType(BoundaryEvent.class)) {
            EscalationEventDefinition escalationDefinition = event.getEventDefinitions().stream()
                    .filter(EscalationEventDefinition.class::isInstance)
                    .map(EscalationEventDefinition.class::cast)
                    .findFirst()
                    .orElse(null);
            if (escalationDefinition == null || event.getAttachedTo() == null) {
                continue;
            }
            String name = normalize(nameOrId(event.getName(), event.getId()));
            String attachedActivityId = normalize(nameOrId(event.getAttachedTo().getName(), event.getAttachedTo().getId()));
            String escalationCode = escalationDefinition.getEscalation() != null
                    && escalationDefinition.getEscalation().getEscalationCode() != null
                    ? normalize(escalationDefinition.getEscalation().getEscalationCode())
                    : name + "_escalation";
            specs.add(new BoundaryEscalationSpec(event.getId(), name, attachedActivityId, escalationCode, event.cancelActivity()));
            nodes.put(event.getId(), new NodeInfo(event.getId(), name, NodeType.BOUNDARY_ESCALATION));
        }
        return specs;
    }

    private static String enclosingSubProcessId(ModelElementInstance element) {
        ModelElementInstance current = element != null ? element.getParentElement() : null;
        while (current != null) {
            if (current instanceof SubProcess subProcess) {
                return subProcess.getId();
            }
            current = current.getParentElement();
        }
        return null;
    }

    private static boolean isWithinSubProcess(ModelElementInstance element, String subProcessId) {
        ModelElementInstance current = element != null ? element.getParentElement() : null;
        while (current != null) {
            if (current instanceof SubProcess subProcess && subProcessId.equals(subProcess.getId())) {
                return true;
            }
            current = current.getParentElement();
        }
        return false;
    }

    private static boolean isEventSubProcess(SubProcess subProcess) {
        if (subProcess == null) {
            return false;
        }
        String value = subProcess.getAttributeValue("triggeredByEvent");
        return value != null && Boolean.parseBoolean(value);
    }

    private static boolean isInterruptingStart(StartEvent startEvent) {
        if (startEvent == null) {
            return false;
        }
        String value = startEvent.getAttributeValue("isInterrupting");
        return value == null || value.isBlank() || Boolean.parseBoolean(value);
    }

    private static void linkSubProcessEntries(
            NodeInfo sourceInfo,
            List<SubProcessSpec> subProcessSpecs,
            String subProcessId,
            FlowInfo flowInfo,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        if (sourceInfo == null) {
            return;
        }
        SubProcessSpec spec = findSubProcessSpec(subProcessSpecs, subProcessId);
        if (spec == null) {
            return;
        }
        for (String entryTargetId : spec.entryTargetIds) {
            NodeInfo targetInfo = nodes.get(entryTargetId);
            if (targetInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(new FlowInfo(flowInfo.id, targetInfo.id, flowInfo.condition));
        }
    }

    private static void linkSubProcessExits(
            NodeInfo targetInfo,
            List<SubProcessSpec> subProcessSpecs,
            String subProcessId,
            FlowInfo flowInfo,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        if (targetInfo == null) {
            return;
        }
        SubProcessSpec spec = findSubProcessSpec(subProcessSpecs, subProcessId);
        if (spec == null) {
            return;
        }
        for (String exitSourceId : spec.exitSourceIds) {
            NodeInfo sourceInfo = nodes.get(exitSourceId);
            if (sourceInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(new FlowInfo(flowInfo.id, targetInfo.id, flowInfo.condition));
        }
    }

    private static SubProcessSpec findSubProcessSpec(List<SubProcessSpec> subProcessSpecs, String subProcessId) {
        for (SubProcessSpec spec : subProcessSpecs) {
            if (spec.id.equals(subProcessId)) {
                return spec;
            }
        }
        return null;
    }

    private static List<String> distinct(List<String> values) {
        List<String> distinct = new ArrayList<>();
        for (String value : values) {
            if (!distinct.contains(value)) {
                distinct.add(value);
            }
        }
        return distinct;
    }

    private static TimerSpec timerSpec(String id, String name, TimerEventDefinition definition) {
        TimeDuration timeDuration = definition.getTimeDuration();
        if (timeDuration != null && timeDuration.getTextContent() != null) {
            return new TimerSpec(id, name, "timeDuration", timeDuration.getTextContent().trim());
        }
        TimeDate timeDate = definition.getTimeDate();
        if (timeDate != null && timeDate.getTextContent() != null) {
            return new TimerSpec(id, name, "timeDate", timeDate.getTextContent().trim());
        }
        TimeCycle timeCycle = definition.getTimeCycle();
        if (timeCycle != null && timeCycle.getTextContent() != null) {
            return new TimerSpec(id, name, "timeCycle", timeCycle.getTextContent().trim());
        }
        return new TimerSpec(id, name, "timeDuration", "PT0S");
    }

    private static <T extends Task> void registerTaskSpecs(
            Iterable<T> tasks,
            TaskKind kind,
            Map<String, TaskSpec> taskSpecs,
            Map<String, NodeInfo> nodes
    ) {
        for (T task : tasks) {
            String name = normalize(nameOrId(task.getName(), task.getId()));
            taskSpecs.put(task.getId(), new TaskSpec(task.getId(), name, kind));
            nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK, kind));
        }
    }

    static String templateForTask(TaskKind kind, boolean transactions) {
        if (kind == TaskKind.USER) {
            return "userTaskHandlerClass";
        }
        if (kind == TaskKind.MANUAL) {
            return "manualTaskHandlerClass";
        }
        if (kind == TaskKind.CALL) {
            return "callActivityTaskStubClass";
        }
        return transactions ? "transactionalWorkerClass" : "workerClass";
    }

    static String classSuffixForTask(TaskKind kind, boolean transactions) {
        return switch (kind) {
            case USER -> "UserTaskService";
            case MANUAL -> "ManualTaskService";
            case CALL -> "CallActivityStub";
            case SERVICE, GENERIC -> transactions ? "TransactionalWorker" : "WorkerService";
        };
    }

    private static void generateTimerHandlers(
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
            if (timerNode == null || timerNode.incomingIds.size() != 1 || timerNode.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, timerNode.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, timerNode.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(timer.name) + "TimerService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateMessageCatchHandlers(
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
            if (node == null || node.incomingIds.size() != 1 || node.outgoingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "MessageCatchService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateMessageThrowHandlers(
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
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "MessageThrowService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateMessageEventPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "MessageEventPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("messageEventPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateSignalEventPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "SignalEventPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("signalEventPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateSignalCatchHandlers(
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
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "SignalCatchService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateSignalThrowHandlers(
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
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "SignalThrowService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateBoundaryErrorHandlers(
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
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "BoundaryErrorService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateBoundaryEscalationHandlers(
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
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "BoundaryEscalationService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateBoundaryTimerHandlers(
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
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "BoundaryTimerService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static void generateCallActivityHandlers(
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
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, node.incomingIds.getFirst());
            Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, node.outgoingIds);
            if (inputChannel.isEmpty() || output.isEmpty()) {
                continue;
            }
            String className = toClassName(spec.name) + "CallActivityService";
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
                writeFile(outputFile, template.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }
    }

    private static String renderYamlPreview(
            STGroupString group,
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageTopics,
            List<String> callActivities,
            List<String> subProcesses
    ) {
        ST yaml = group.getInstanceOf("applicationYaml");
        yaml.add("processId", processId);
        yaml.add("tasks", tasks);
        yaml.add("timers", timers);
        yaml.add("messageTopics", messageTopics);
        yaml.add("callActivities", callActivities);
        yaml.add("subProcesses", subProcesses);
        return yaml.render();
    }

    private static String renderTopicsPreview(
            STGroupString group,
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageTopics,
            List<String> callActivities,
            List<String> subProcesses
    ) {
        ST topicsScript = group.getInstanceOf("topicsScript");
        topicsScript.add("processId", processId);
        topicsScript.add("tasks", tasks);
        topicsScript.add("timers", timers);
        topicsScript.add("messageTopics", messageTopics);
        topicsScript.add("callActivities", callActivities);
        topicsScript.add("subProcesses", subProcesses);
        return topicsScript.render();
    }

    private static String renderPomPreview(STGroupString group, String processId) {
        ST pom = group.getInstanceOf("pomXml");
        pom.add("processId", processId);
        pom.add("starterClass", toClassName(processId));
        return pom.render();
    }

    private static String renderRunLocalPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("runLocalScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderDemoPreview(STGroupString group, String processId, List<String> tasks) {
        ST script = group.getInstanceOf("demoScenarioScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        script.add("firstTask", tasks.isEmpty() ? "start" : tasks.getFirst());
        script.add("lastTask", tasks.isEmpty() ? "completed" : tasks.getLast());
        return script.render();
    }

    private static String renderTaskInputPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("taskInputScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderTaskCompletionPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("taskCompletionScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderTaskFailurePreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("taskFailureScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderTaskEscalationPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("taskEscalationScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderCallActivityCompletionPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("callActivityCompletionScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderMessageEventPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("messageEventScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderSignalEventPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("signalEventScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderProcessEventsWatchPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("processEventsWatchScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static String renderTaskOutputWatchPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("taskOutputWatchScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static void generateCoreClasses(
            STGroupString group,
            Path coreJavaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessEvent.java", "processEventClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessState.java", "processStateClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessStateStore.java", "processStateStoreClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ScopeCancellationRegistry.java", "scopeCancellationRegistryClass");
    }

    private static void writeCoreClass(
            STGroupString group,
            Path coreJavaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String fileName,
            String templateName
    ) {
        Path outputFile = coreJavaOutput.resolve(fileName);
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf(templateName);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateStarter(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "Starter";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("starterClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateDemoRunner(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId,
            List<String> tasks
    ) {
        String className = toClassName(processId) + "DemoScenarioRunner";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("demoScenarioRunnerClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        template.add("firstTask", tasks.isEmpty() ? "start" : tasks.getFirst());
        template.add("lastTask", tasks.isEmpty() ? "completed" : tasks.getLast());
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateTaskInputPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "TaskInputPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("taskInputPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateTaskCompletionPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "TaskCompletionPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("taskCompletionPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateTaskFailurePublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "TaskFailurePublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("taskFailurePublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateTaskEscalationPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "TaskEscalationPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("taskEscalationPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateCallActivityCompletionPublisher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "CallActivityCompletionPublisher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("callActivityCompletionPublisherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateProcessEventsWatcher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "ProcessEventsWatcher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("processEventsWatcherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateTaskOutputWatcher(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "TaskOutputWatcher";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("taskOutputWatcherClass");
        template.add("packageName", "org.gautelis.durga.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    static String toClassName(String value) {
        String[] parts = normalize(value).split("_");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase(Locale.ROOT))
                    .append(part.substring(1));
        }
        return builder.length() == 0 ? "Unnamed" : builder.toString();
    }

    private static String loadTemplates() {
        return loadTemplateResources(
                "/templates/scaffold.stg",
                "/templates/scaffold-events.stg",
                "/templates/scaffold-subprocess.stg",
                "/templates/scaffold-generated-project.stg"
        );
    }

    private static String loadTemplateResources(String... resources) {
        StringBuilder templates = new StringBuilder();
        for (String resource : resources) {
            try (InputStream input = BpmnScaffolder.class.getResourceAsStream(resource)) {
                if (input == null) {
                    throw new IllegalStateException("Template not found: " + resource);
                }
                if (templates.length() > 0) {
                    templates.append('\n');
                }
                templates.append(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load templates from " + resource, e);
            }
        }
        return templates.toString();
    }

    static void writeFile(Path path, String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private static FlowInfo flowInfo(SequenceFlow flow) {
        ConditionExpression expression = flow.getConditionExpression();
        String condition = expression != null ? expression.getTextContent() : null;
        String normalized = condition != null ? condition.trim() : null;
        return new FlowInfo(flow.getId(), flow.getTarget().getId(), normalized);
    }

    private static FlowInfo findFlowInfo(List<FlowInfo> flows, String targetId) {
        if (flows == null) {
            return null;
        }
        for (FlowInfo flow : flows) {
            if (flow.targetId.equals(targetId)) {
                return flow;
            }
        }
        return null;
    }

    private static Set<String> indexSourceFiles(Path root) {
        Set<String> files = new TreeSet<>();
        if (!Files.exists(root)) {
            return files;
        }
        try {
            Files.walk(root)
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(path -> files.add(path.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to index source files", e);
        }
        return files;
    }

    static List<OutputSpec> withConditionBlocks(List<OutputSpec> outputs) {
        List<OutputSpec> updated = new ArrayList<>();
        boolean hasDefault = outputs.stream().anyMatch(output -> output.isDefault);
        boolean hasCondition = outputs.stream().anyMatch(output -> output.condition != null);
        for (int i = 0; i < outputs.size(); i++) {
            OutputSpec output = outputs.get(i);
            String conditionExpr;
            if (output.condition != null) {
                conditionExpr = "evaluateCondition(\"" + escapeJava(output.condition) + "\", inputEvent)";
            } else if (output.isDefault) {
                conditionExpr = "true";
            } else if (!hasCondition && !hasDefault && i == 0) {
                conditionExpr = "true";
            } else {
                continue;
            }
            String block = "        if (" + conditionExpr + ") {\n"
                    + "            " + output.emitter + ".send(msg.getPayload());\n"
                    + "            processEventsEmitter.send(new ProcessEvent(\n"
                    + "                    inputEvent.processInstanceId(),\n"
                    + "                    inputEvent.processId(),\n"
                    + "                    \"" + output.taskId + "\",\n"
                    + "                    inputEvent.tokenId(),\n"
                    + "                    inputEvent.correlationId(),\n"
                    + "                    inputEvent.payload(),\n"
                    + "                    ProcessEvent.Status.STARTED,\n"
                    + "                    null,\n"
                    + "                    ProcessEvent.EventType.GATEWAY_TAKEN,\n"
                    + "                    inputEvent.processVersion(),\n"
                    + "                    inputEvent.businessKey(),\n"
                    + "                    null\n"
                    + "            ).toJson());\n"
                    + "            return msg.ack();\n"
                    + "        }\n";
            updated.add(new OutputSpec(output.emitter, output.channel, output.taskId, output.nodeType, output.condition,
                    output.isDefault, block));
        }
        return updated;
    }

    private static String escapeJava(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

}
