package org.gautelis.durga.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.BoundaryEvent;
import org.camunda.bpm.model.bpmn.instance.BusinessRuleTask;
import org.camunda.bpm.model.bpmn.instance.CallActivity;
import org.camunda.bpm.model.bpmn.instance.ConditionExpression;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.EscalationEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ErrorEventDefinition;
import org.camunda.bpm.model.bpmn.instance.ExclusiveGateway;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.InclusiveGateway;
import org.camunda.bpm.model.bpmn.instance.ManualTask;
import org.camunda.bpm.model.bpmn.instance.IntermediateCatchEvent;
import org.camunda.bpm.model.bpmn.instance.IntermediateThrowEvent;
import org.camunda.bpm.model.bpmn.instance.MessageEventDefinition;
import org.camunda.bpm.model.bpmn.instance.MultiInstanceLoopCharacteristics;
import org.camunda.bpm.model.bpmn.instance.ParallelGateway;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.camunda.bpm.model.bpmn.instance.ReceiveTask;
import org.camunda.bpm.model.bpmn.instance.ScriptTask;
import org.camunda.bpm.model.bpmn.instance.SendTask;
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
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperties;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperty;
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
import java.util.stream.Stream;

/**
 * Scaffolds a BPMN model into an explicit Durga/Kafka runtime project.
 * <p>
 * The generator favors inspectable source code and helper scripts over a hidden workflow engine,
 * so the generated project can be studied, modified, and demoed locally.
 */
public class BpmnScaffolder {
    private static final Logger LOG = LoggerFactory.getLogger(BpmnScaffolder.class);
    static String generatedPackage = "org.example.generated";
    static String generatedProbesPackage;
    static String eventsTopic;

    /**
     * Generates a project from a BPMN file.
     *
     * @param args command-line arguments such as {@code --dry-run}, {@code --output-dir}, and the
     *             BPMN file path
     */
    public static void main(String[] args) {
        ParsedArgs parsed = CliParser.parse(args);
        if (parsed == null) {
            System.exit(1);
        }

        File bpmnFile = new File(parsed.bpmnPath);
        if (!bpmnFile.exists()) {
            LOG.error("BPMN file not found: {}", bpmnFile.getAbsolutePath());
            System.exit(1);
        }

        Path outputRoot = SafeXml.safePath(parsed.outputDir);
        String pkg = parsed.packageName;
        if (pkg == null || pkg.isBlank()) {
            pkg = generatedPackage;
            LOG.warn("Warning: no --package specified, defaulting to " + pkg);
        }
        generatedPackage = pkg;
        generatedProbesPackage = pkg + ".probes";
        eventsTopic = parsed.eventsTopic;
        Path javaOutput = outputRoot.resolve("src/main/java/" + pkg.replace('.', '/'));

        String retention = parsed.retentionHours;
        long retentionMs;
        if (retention == null || retention.isBlank()) {
            retentionMs = 168 * 3600 * 1000L; // 7 days
            LOG.warn("Warning: no --retention specified, defaulting to 168h (7 days). "
                    + "Topics older than this will be deleted. "
                    + "Use --retention 42h|5d|2w|1m or -1 for infinite.");
        } else if ("-1".equals(retention)) {
            retentionMs = -1;
            LOG.warn("Note: retention set to -1 (infinite). Topics will never be deleted.");
        } else {
            retentionMs = parseRetention(retention);
        }
        Path probesOutput = outputRoot.resolve("src/main/java/" + pkg.replace('.', '/') + "/probes");
        Path coreJavaOutput = outputRoot.resolve("src/main/java/org/gautelis/durga");
        Path mainSourceRoot = Path.of("src/main/java");

        BpmnModelInstance model = SafeXml.readModelFromFile(bpmnFile);
        Process process = model.getModelElementsByType(Process.class).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No <process> element found."));

        String processId = normalize(
                parsed.processIdOverride != null
                        ? parsed.processIdOverride
                        : (process.getId() != null ? process.getId() : "process")
        );
        if (eventsTopic == null || eventsTopic.isBlank()) {
            eventsTopic = "process-events-" + processId;
        }

        String rawProcessSource = parsed.processIdOverride != null
                ? parsed.processIdOverride
                : (process.getId() != null ? process.getId() : "process");
        List<NameValidation.Issue> nameIssues = NameValidation.validate(model, rawProcessSource);
        for (NameValidation.Issue issue : nameIssues) {
            if (issue.severity() == NameValidation.Severity.WARNING) {
                System.err.println("Name check (warning): " + issue.message());
                LOG.warn("Name check: {}", issue.message());
            }
        }
        if (NameValidation.hasErrors(nameIssues)) {
            for (NameValidation.Issue issue : nameIssues) {
                if (issue.severity() == NameValidation.Severity.ERROR) {
                    System.err.println("Name check (error): " + issue.message());
                    LOG.error("Name check: {}", issue.message());
                }
            }
            System.err.println("Aborting: BPMN names do not yield valid identifiers/topics. "
                    + "Rename the offending elements or pass --process-id with a valid id.");
            System.exit(1);
        }
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        Map<String, List<FlowInfo>> flowsBySource = new LinkedHashMap<>();
        List<EventSubProcessSpec> eventSubProcessSpecs = BpmnModelCollector.collectEventSubProcessSpecs(model, nodes);
        List<SubProcessSpec> subProcessSpecs = BpmnModelCollector.collectSubProcessSpecs(model, nodes);
        List<String> subProcesses = subProcessSpecs.stream().map(spec -> spec.name).toList();
        List<TaskSpec> taskSpecs = BpmnModelCollector.collectTaskSpecs(model, nodes);
        List<String> tasks = taskSpecs.stream().map(task -> task.name).toList();
        List<DataObjectSpec> dataObjectSpecs = BpmnModelCollector.collectDataObjectSpecs(model);
        List<String> dataObjects = dataObjectSpecs.stream().map(spec -> spec.name).toList();
        List<DataStoreSpec> dataStoreSpecs = BpmnModelCollector.collectDataStoreSpecs(model);
        List<String> dataStores = dataStoreSpecs.stream().map(spec -> spec.name).toList();
        List<DataAssociationSpec> dataAssociationSpecs = BpmnModelCollector.collectDataAssociationSpecs(model);
        validateDataObjectSchemas(dataObjectSpecs, outputRoot);
        List<TimerSpec> timerSpecs = BpmnModelCollector.collectTimerSpecs(model, nodes);
        List<String> timers = timerSpecs.stream().map(timer -> timer.name).toList();
        List<BoundaryTimerSpec> boundaryTimerSpecs = BpmnModelCollector.collectBoundaryTimerSpecs(model, nodes);
        List<String> boundaryTimers = boundaryTimerSpecs.stream().map(timer -> timer.name).toList();
        List<BoundaryErrorSpec> boundaryErrorSpecs = BpmnModelCollector.collectBoundaryErrorSpecs(model, nodes);
        List<String> boundaryErrors = boundaryErrorSpecs.stream().map(error -> error.name).toList();
        List<BoundaryEscalationSpec> boundaryEscalationSpecs = BpmnModelCollector.collectBoundaryEscalationSpecs(model, nodes);
        List<String> boundaryEscalations = boundaryEscalationSpecs.stream().map(escalation -> escalation.name).toList();
        List<MessageCatchSpec> messageCatchSpecs = BpmnModelCollector.collectMessageCatchSpecs(model, nodes);
        List<MessageThrowSpec> messageThrowSpecs = BpmnModelCollector.collectMessageThrowSpecs(model, nodes);
        List<String> messageEvents = BpmnModelCollector.collectMessageEvents(messageCatchSpecs, messageThrowSpecs, eventSubProcessSpecs);
        List<String> messageTopics = BpmnModelCollector.collectMessageTopics(messageCatchSpecs, messageThrowSpecs, eventSubProcessSpecs);
        List<SignalCatchSpec> signalCatchSpecs = BpmnModelCollector.collectSignalCatchSpecs(model, nodes);
        List<SignalThrowSpec> signalThrowSpecs = BpmnModelCollector.collectSignalThrowSpecs(model, nodes);
        List<String> signalEvents = BpmnModelCollector.collectSignalEvents(signalCatchSpecs, signalThrowSpecs, eventSubProcessSpecs);
        List<String> signalTopics = BpmnModelCollector.collectSignalTopics(signalCatchSpecs, signalThrowSpecs, eventSubProcessSpecs);
        List<CallActivitySpec> callActivitySpecs = BpmnModelCollector.collectCallActivitySpecs(model, nodes);
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

        List<NodeInfo> ors = new ArrayList<>();
        for (InclusiveGateway gateway : model.getModelElementsByType(InclusiveGateway.class)) {
            NodeInfo info = new NodeInfo(gateway.getId(), normalize(nameOrId(gateway.getName(), gateway.getId())),
                    NodeType.OR);
            if (gateway.getDefault() != null) {
                info.defaultFlowId = gateway.getDefault().getId();
            }
            nodes.put(gateway.getId(), info);
            ors.add(info);
        }

        List<MultiInstanceSpec> multiInstanceSpecs = BpmnModelCollector.collectMultiInstanceSpecs(model);

        for (StartEvent startEvent : model.getModelElementsByType(StartEvent.class)) {
            if (BpmnModelCollector.enclosingSubProcessId(startEvent) != null) {
                continue;
            }
            nodes.put(startEvent.getId(), new NodeInfo(startEvent.getId(), "start", NodeType.START));
        }

        for (EndEvent endEvent : model.getModelElementsByType(EndEvent.class)) {
            if (BpmnModelCollector.enclosingSubProcessId(endEvent) != null) {
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

        // Rust code-generation target: from the same extracted model, render a Cargo
        // project whose workers depend on the durga-rust plugin crate. This is a distinct
        // generation path from the Java target below.
        if ("rust".equals(parsed.target)) {
            RustTargetGenerator.generate(parsed, processId, outputRoot, taskSpecs, nodes, flowsBySource);
            return;
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
        generateCoreClasses(group, coreJavaOutput, outputRoot, generatedFiles, parsed.dryRun, parsed, processId);

        Map<String, TaskLineage> taskLineage = buildTaskLineage(taskSpecs, dataAssociationSpecs);

        TaskRoutingGenerator.generateTaskHandlers(
                group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, parsed.transactions,
                processId, taskSpecs, existingSources, taskLineage, nodes, parsed.validation
        );

        generateStarter(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateDemoRunner(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId, tasks);
        generateTaskInputPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskCompletionPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskFailurePublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskEscalationPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateCallActivityCompletionPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
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
        EventTemplateGenerator.generateMessageEventPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        EventTemplateGenerator.generateSignalEventPublisher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateProcessEventsWatcher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);
        generateTaskOutputWatcher(group, probesOutput, outputRoot, generatedFiles, parsed.dryRun, processId);

        TaskRoutingGenerator.generateGateways(
                processId, group, javaOutput, nodes, flowsBySource, xors, ands, ors, existingSources,
                outputRoot, generatedFiles, parsed.dryRun
        );
        TaskRoutingGenerator.generateOrchestrator(
                processId, group, javaOutput, nodes, existingSources, outputRoot, generatedFiles, parsed.dryRun
        );

        List<String> allTimers = combineNames(timers, boundaryTimers);
        List<String> externalTopics = combineNames(messageTopics, signalTopics);
        List<String> validationTasks = parsed.validation
                ? taskSpecs.stream()
                        .filter(t -> t.kind == TaskKind.PLUGIN && t.pluginRef != null)
                        .map(t -> t.name)
                        .toList()
                : List.of();
        String yamlPreview = renderYamlPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses);
        String topicsPreview = renderTopicsPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses, retentionMs, !validationTasks.isEmpty());
        String bpmnFileName = bpmnFile.toPath().getFileName().toString();
        String pomPreview = renderPomPreview(group, processId, bpmnFileName);
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
        String replayRunbookPreview = renderReplayRunbookPreview(group, processId, tasks);
        String payloadPreview = GeneratedProjectSupport.renderTaskPayloadsPreview(processId, tasks);
        Map<String, String> taskInputChannels = computeTaskInputChannels(processId, nodes);
        if (!parsed.dryRun) {
            // application.yml is merged rather than replaced so the generated project can keep
            // accumulating channel config across repeated scaffold runs.
            GeneratedProjectSupport.mergeApplicationYaml(
                    processId, tasks, allTimers, externalTopics, callActivities, subProcesses, outputRoot,
                    taskInputChannels, validationTasks
            );
            Path topicsPath = outputRoot.resolve("topics.sh");
            writeFile(topicsPath, topicsPreview);
            generatedFiles.add(outputRoot.relativize(topicsPath).toString());

            if (parsed.strimzi) {
                ST strimziSt = group.getInstanceOf("topicsYml");
                strimziSt.add("processId", processId);
                strimziSt.add("tasks", tasks);
                strimziSt.add("timers", allTimers);
                strimziSt.add("messageTopics", messageTopics);
                strimziSt.add("callActivities", callActivities);
                strimziSt.add("subProcesses", subProcesses);
                strimziSt.add("retentionMs", retentionMs);
                Path topicsYmlPath = outputRoot.resolve("topics.yml");
                writeFile(topicsYmlPath, strimziSt.render());
                generatedFiles.add(outputRoot.relativize(topicsYmlPath).toString());
            }

            if (parsed.strimzi || parsed.separateWorkers) {
                ST kedaSt = group.getInstanceOf("kedaScalers");
                kedaSt.add("processId", processId);
                kedaSt.add("tasks", tasks);
                Path kedaPath = outputRoot.resolve("keda-scalers.yml");
                writeFile(kedaPath, kedaSt.render());
                generatedFiles.add(outputRoot.relativize(kedaPath).toString());
            }

            ST aclsSt = group.getInstanceOf("aclsScript");
            aclsSt.add("processId", processId);
            aclsSt.add("tasks", tasks);
            aclsSt.add("messageTopics", messageTopics);
            aclsSt.add("callActivities", callActivities);
            aclsSt.add("subProcesses", subProcesses);
            Path aclsPath = outputRoot.resolve("acls.sh");
            writeFile(aclsPath, aclsSt.render());
            generatedFiles.add(outputRoot.relativize(aclsPath).toString());

            ST alertsSt = group.getInstanceOf("prometheusAlertsYml");
            alertsSt.add("processId", processId);
            alertsSt.add("tasks", tasks);
            Path alertsPath = outputRoot.resolve("alerts.yml");
            writeFile(alertsPath, alertsSt.render());
            generatedFiles.add(outputRoot.relativize(alertsPath).toString());

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

            Path replayPath = outputRoot.resolve("replay.sh");
            writeFile(replayPath, replayRunbookPreview);
            generatedFiles.add(outputRoot.relativize(replayPath).toString());

            Path dockerfilePath = outputRoot.resolve("Dockerfile");
            try {
                writeFile(dockerfilePath, renderDockerfile(group, processId));
                generatedFiles.add(outputRoot.relativize(dockerfilePath).toString());
            } catch (Exception e) {
                LOG.warn("Warning: failed to generate Dockerfile: " + e.getMessage());
            }

            Path k8sPath = outputRoot.resolve("k8s.yml");
            try {
                writeFile(k8sPath, renderK8s(group, processId));
                generatedFiles.add(outputRoot.relativize(k8sPath).toString());
            } catch (Exception e) {
                LOG.warn("Warning: failed to generate k8s.yml: " + e.getMessage());
            }

            Path deployPath = outputRoot.resolve("deploy.sh");
            try {
                writeFile(deployPath, renderDeployScript(group, processId));
                generatedFiles.add(outputRoot.relativize(deployPath).toString());
            } catch (Exception e) {
                LOG.warn("Warning: failed to generate deploy.sh: " + e.getMessage());
            }

            Path composeTestPath = outputRoot.resolve("docker-compose.test.yml");
            try {
                writeFile(composeTestPath, renderDockerComposeTest(group, processId));
                generatedFiles.add(outputRoot.relativize(composeTestPath).toString());
            } catch (Exception e) {
                LOG.warn("Warning: failed to generate docker-compose.test.yml: " + e.getMessage());
            }

            Path payloadPath = outputRoot.resolve("task-payloads.json");
            writeFile(payloadPath, payloadPreview);
            generatedFiles.add(outputRoot.relativize(payloadPath).toString());
        }

        if (parsed.separateWorkers && !parsed.dryRun) {
            for (String task : tasks) {
                String workerClass = toClassName(task) + "Standalone";
                Path workerPath = javaOutput.resolve(workerClass + ".java");
                if (!Files.exists(workerPath) && !existingSources.contains(workerClass + ".java")) {
                    ST workerSt = group.getInstanceOf("workerStandaloneClass");
                    workerSt.add("packageName", generatedPackage);
                    workerSt.add("className", toClassName(task));
                    workerSt.add("processId", processId);
                    workerSt.add("taskId", task);
                    workerSt.add("inputChannel", taskInputChannels.getOrDefault(task, processId + "_" + task + "_input"));
                    writeFile(workerPath, workerSt.render());
                    generatedFiles.add(outputRoot.relativize(workerPath).toString());
                }
            }

            String bootstrapClass = toClassName(processId) + "WorkerBootstrap";
            Path bootstrapPath = javaOutput.resolve(bootstrapClass + ".java");
            if (!Files.exists(bootstrapPath) && !existingSources.contains(bootstrapClass + ".java")) {
                ST bootstrapSt = group.getInstanceOf("workerBootstrapMain");
                bootstrapSt.add("packageName", generatedPackage);
                bootstrapSt.add("className", bootstrapClass);
                bootstrapSt.add("processId", processId);
                bootstrapSt.add("tasks", tasks);
                writeFile(bootstrapPath, bootstrapSt.render());
                generatedFiles.add(outputRoot.relativize(bootstrapPath).toString());
            }

            for (String task : tasks) {
                Path wDockerfile = outputRoot.resolve("Dockerfile." + task);
                ST wDfSt = group.getInstanceOf("perWorkerDockerfile");
                wDfSt.add("processId", processId);
                wDfSt.add("taskId", task);
                writeFile(wDockerfile, wDfSt.render());
                generatedFiles.add(outputRoot.relativize(wDockerfile).toString());

                Path wK8s = outputRoot.resolve("k8s." + task + ".yml");
                ST wK8sSt = group.getInstanceOf("perWorkerK8s");
                wK8sSt.add("processId", processId);
                wK8sSt.add("taskId", task);
                writeFile(wK8s, wK8sSt.render());
                generatedFiles.add(outputRoot.relativize(wK8s).toString());
            }
        }

        if (parsed.connect) {
            List<String> outputTopics = collectTerminalOutputs(nodes);
            List<DataStoreConnectSpec> dataStoreConnectSpecs = collectDataStoreConnectSpecs(
                    processId, dataStoreSpecs, dataAssociationSpecs
            );

            StringBuilder sourceTopics = new StringBuilder();
            sourceTopics.append(processId).append("_start");
            if (!messageTopics.isEmpty()) {
                sourceTopics.append(", ");
                sourceTopics.append(String.join(", ", messageTopics));
            }

            StringBuilder sinkTopics = new StringBuilder();
            sinkTopics.append(eventsTopic);
            for (String topic : outputTopics) {
                sinkTopics.append(", ").append(processId).append("_").append(topic).append("_output");
            }
            for (String topic : messageTopics) {
                sinkTopics.append(", ").append(topic);
            }

            if (parsed.dryRun) {
                ST sourceSt = group.getInstanceOf("connectSourceConfig");
                sourceSt.add("processId", processId);
                sourceSt.add("sourceTopics", sourceTopics.toString());
                System.out.println("--- connect-source.json ---");
                System.out.println(sourceSt.render());

                ST sinkSt = group.getInstanceOf("connectSinkConfig");
                sinkSt.add("processId", processId);
                sinkSt.add("sinkTopics", sinkTopics.toString());
                System.out.println("--- connect-sink.json ---");
                System.out.println(sinkSt.render());

                for (DataStoreConnectSpec spec : dataStoreConnectSpecs) {
                    ST dataStoreSt = group.getInstanceOf("dataStoreConnectConfig");
                    dataStoreSt.add("spec", spec);
                    System.out.println("--- connect/data-stores/" + spec.name + ".json ---");
                    System.out.println(dataStoreSt.render());
                }
            } else {
                Path connectDir = outputRoot.resolve("connect");
                try {
                    Files.createDirectories(connectDir);
                    Files.createDirectories(connectDir.resolve("data-stores"));
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to create connect directory", e);
                }

                ST sourceSt = group.getInstanceOf("connectSourceConfig");
                sourceSt.add("processId", processId);
                sourceSt.add("sourceTopics", sourceTopics.toString());
                Path sourcePath = connectDir.resolve("connect-source.json");
                writeFile(sourcePath, sourceSt.render());
                generatedFiles.add(outputRoot.relativize(sourcePath).toString());

                ST sinkSt = group.getInstanceOf("connectSinkConfig");
                sinkSt.add("processId", processId);
                sinkSt.add("sinkTopics", sinkTopics.toString());
                Path sinkPath = connectDir.resolve("connect-sink.json");
                writeFile(sinkPath, sinkSt.render());
                generatedFiles.add(outputRoot.relativize(sinkPath).toString());

                for (DataStoreConnectSpec spec : dataStoreConnectSpecs) {
                    ST dataStoreSt = group.getInstanceOf("dataStoreConnectConfig");
                    dataStoreSt.add("spec", spec);
                    Path dataStorePath = connectDir.resolve("data-stores").resolve(spec.name + ".json");
                    writeFile(dataStorePath, dataStoreSt.render());
                    generatedFiles.add(outputRoot.relativize(dataStorePath).toString());
                }

                Path connectScriptPath = outputRoot.resolve("connect.sh");
                ST scriptSt = group.getInstanceOf("connectScript");
                scriptSt.add("processId", processId);
                writeFile(connectScriptPath, scriptSt.render());
                generatedFiles.add(outputRoot.relativize(connectScriptPath).toString());
            }
        }

        List<String> boundaryEvents = combineNames(combineNames(boundaryTimers, boundaryErrors), boundaryEscalations);
        Map<String, Object> summary = GeneratedProjectSupport.buildSummary(
                processId, tasks, allTimers, messageEvents, messageTopics, signalEvents, signalTopics,
                boundaryEvents, callActivities, subProcesses, dataObjectSpecs, dataStoreSpecs,
                dataAssociationSpecs, xors, ands, ors, multiInstanceSpecs, generatedFiles, taskLineage
        );
        String summaryJson = GeneratedProjectSupport.renderSummaryJson(summary);

        if (!parsed.dryRun) {
            GeneratedProjectSupport.writeSummary(outputRoot, summaryJson);
            GeneratedProjectSupport.writeGeneratedReadme(
                    outputRoot, processId, tasks, allTimers, messageEvents, messageTopics, signalEvents,
                    signalTopics, boundaryEvents, callActivities, subProcesses, dataObjectSpecs,
                    dataStoreSpecs, dataAssociationSpecs, xors, ands, ors, multiInstanceSpecs,
                    generatedFiles, payloadPreview, taskLineage
            );
            GeneratedProjectSupport.copyBpmnModel(outputRoot, parsed.bpmnPath, generatedFiles);

            Path copiedBpmn = outputRoot.resolve("src/main/resources")
                    .resolve(SafeXml.safePath(parsed.bpmnPath).getFileName());
            try {
                int restored = ModelEnricher.restore(copiedBpmn, outputRoot.resolve("src/main/java"));
                if (restored > 0) {
                    System.out.println("Restored " + restored
                            + " embedded custom source file(s) from BPMN");
                }
            } catch (IOException e) {
                System.err.println("Warning: failed to restore embedded sources: " + e.getMessage());
            }
        }

        System.out.println("Generated in " + outputRoot.toAbsolutePath());
        System.out.println("Process: " + processId);
        System.out.println("Tasks: " + tasks);
        System.out.println("Call activities: " + callActivities);
        System.out.println("Embedded subprocesses: " + subProcesses);
        System.out.println("Data objects: " + dataObjects);
        System.out.println("Data stores: " + dataStores);
        System.out.println("OR gateways: " + ors.stream().map(o -> o.name).toList());
        System.out.println("Multi-instance tasks: " + multiInstanceSpecs.stream().map(m -> m.taskName).toList());
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

    private static String normalize(String value) {
        return BpmnModelCollector.normalize(value);
    }

    private static String nameOrId(String name, String id) {
        return BpmnModelCollector.nameOrId(name, id);
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

    /**
     * Determines the Kafka topic a task worker should consume from based on its BPMN predecessor.
     * <ul>
     *   <li>After START → {@code {processId}_start}</li>
     *   <li>After another TASK → {@code {processId}_{predecessor.name}_output}</li>
     *   <li>After a GATEWAY → {@code {processId}_{task.name}_input} (gateway publishes here)</li>
     * </ul>
     */
    static String resolveTaskInputChannel(String processId, Map<String, NodeInfo> nodes, NodeInfo taskNode) {
        if (taskNode.incomingIds.isEmpty()) {
            return processId + "_" + taskNode.name + "_input";
        }
        String sourceId = taskNode.incomingIds.getFirst();
        NodeInfo source = nodes.get(sourceId);
        if (source == null) {
            return processId + "_" + taskNode.name + "_input";
        }
        return switch (source.type) {
            case START -> processId + "_start";
            case TASK, CALL_ACTIVITY, SUB_PROCESS -> processId + "_" + source.name + "_output";
            case XOR, AND, OR ->
                    processId + "_" + taskNode.name + "_input";
            default -> processId + "_" + taskNode.name + "_input";
        };
    }

    static String inputEmitterChannel(String processId, String activityName) {
        return processId + "_" + activityName + "_input_emit";
    }

    static List<String> resolveFirstTaskNames(Map<String, NodeInfo> nodes) {
        List<String> firstTaskNames = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type != NodeType.START) {
                continue;
            }
            for (String targetId : node.outgoingIds) {
                NodeInfo target = nodes.get(targetId);
                if (target != null && target.type == NodeType.TASK) {
                    firstTaskNames.add(target.name);
                }
            }
        }
        return firstTaskNames;
    }

    static Map<String, String> computeTaskInputChannels(String processId, Map<String, NodeInfo> nodes) {
        Map<String, String> channels = new LinkedHashMap<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type != NodeType.TASK) {
                continue;
            }
            channels.put(node.name, resolveTaskInputChannel(processId, nodes, node));
        }
        return channels;
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
                    inputEmitterChannel(processId, target.name),
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
        String channel = target.type == NodeType.END ? "process-events" : inputEmitterChannel(processId, target.name);
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

    private static List<String> combineNames(List<String> primary, List<String> secondary) {
        return BpmnModelCollector.combineNames(primary, secondary);
    }

    private static List<String> distinct(List<String> values) {
        return BpmnModelCollector.distinct(values);
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
        if (kind == TaskKind.SEND) {
            return "sendTaskClass";
        }
        if (kind == TaskKind.RECEIVE) {
            return "receiveTaskClass";
        }
        if (kind == TaskKind.PLUGIN) {
            return "pluginExecutorClass";
        }
        if (kind == TaskKind.CUSTOM) {
            return "customDelegatingWorkerClass";
        }
        return transactions ? "transactionalWorkerClass" : "workerClass";
    }

    static String classSuffixForTask(TaskKind kind, boolean transactions) {
        return switch (kind) {
            case USER -> "UserTaskService";
            case MANUAL -> "ManualTaskService";
            case CALL -> "CallActivityStub";
            case SEND -> "SendTaskService";
            case RECEIVE -> "ReceiveTaskService";
            case SCRIPT, BUSINESS_RULE, CUSTOM -> "WorkerService";
            case PLUGIN -> "PluginExecutor";
            case SERVICE, GENERIC -> transactions ? "TransactionalWorker" : "WorkerService";
        };
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
        yaml.add("eventsTopic", eventsTopic);
        return yaml.render();
    }

    private static String renderTopicsPreview(
            STGroupString group,
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageTopics,
            List<String> callActivities,
            List<String> subProcesses,
            long retentionMs,
            boolean validationEnabled
    ) {
        ST topicsScript = group.getInstanceOf("topicsScript");
        topicsScript.add("processId", processId);
        topicsScript.add("tasks", tasks);
        topicsScript.add("timers", timers);
        topicsScript.add("messageTopics", messageTopics);
        topicsScript.add("callActivities", callActivities);
        topicsScript.add("subProcesses", subProcesses);
        topicsScript.add("retentionMs", retentionMs);
        topicsScript.add("eventsTopic", eventsTopic);
        topicsScript.add("validationEnabled", validationEnabled);
        return topicsScript.render();
    }

    private static String renderPomPreview(STGroupString group, String processId, String bpmnFileName) {
        ST pom = group.getInstanceOf("pomXml");
        pom.add("processId", processId);
        pom.add("starterClass", toClassName(processId));
        pom.add("bpmnFileName", bpmnFileName);
        pom.add("durgaVersion", durgaVersion());
        return pom.render();
    }

    private static String durgaVersion() {
        Package pkg = BpmnScaffolder.class.getPackage();
        return pkg != null && pkg.getImplementationVersion() != null
                ? pkg.getImplementationVersion()
                : "0.1.0-beta.1";
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

    private static String renderReplayRunbookPreview(STGroupString group, String processId, List<String> tasks) {
        ST script = group.getInstanceOf("replayRunbookScript");
        script.add("processId", processId);
        script.add("tasks", tasks);
        return script.render();
    }

    private static String renderDockerfile(STGroupString group, String processId) {
        ST tmpl = group.getInstanceOf("dockerfileTemplate");
        tmpl.add("className", toClassName(processId));
        return tmpl.render();
    }

    private static String renderK8s(STGroupString group, String processId) {
        ST tmpl = group.getInstanceOf("kubernetesTemplate");
        tmpl.add("processId", processId);
        return tmpl.render();
    }

    private static String renderDeployScript(STGroupString group, String processId) {
        ST tmpl = group.getInstanceOf("deployScript");
        tmpl.add("processId", processId);
        return tmpl.render();
    }

    private static String renderDockerComposeTest(STGroupString group, String processId) {
        ST tmpl = group.getInstanceOf("dockerComposeTest");
        tmpl.add("processId", processId);
        return tmpl.render();
    }

    private static void generateCoreClasses(
            STGroupString group,
            Path coreJavaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            ParsedArgs parsed,
            String processId
    ) {
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessEvent.java", "processEventClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessState.java", "processStateClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "DataHandle.java", "dataHandleClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "DataIndividualMetadataEvent.java", "dataIndividualMetadataEventClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "VannakMetadata.java", "vannakMetadataClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessStateStore.java", "processStateStoreClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ScopeCancellationRegistry.java", "scopeCancellationRegistryClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "tools/ModelEnricher.java", "modelEnricherClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/Plugin.java", "pluginInterfaceClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/PluginResult.java", "pluginResultClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/PipelinePlugin.java", "pipelinePluginClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/PluginExecutionSupport.java", "pluginExecutionSupportClass");

        if (parsed.validation) {
            writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                    "validation/ValidationCandidateOutput.java", "validationCandidateOutputClass");
        }

        // Model registration bean — publishes BPMN to process-models topic on startup
        {
            String modelFileName = SafeXml.safePath(parsed.bpmnPath).getFileName().toString();
            Path outputFile = coreJavaOutput.resolve("ModelRegistration.java");
            if (!Files.exists(outputFile)) {
                ST template = group.getInstanceOf("modelRegistrationClass");
                template.add("packageName", generatedPackage);
                template.add("processId", processId);
                template.add("bpmnFileName", modelFileName);
                if (!dryRun) {
                    writeFile(outputFile, template.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            }
        }
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        String className = toClassName(processId) + "Runner";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("demoScenarioRunnerClass");
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("firstTask", tasks.isEmpty() ? "start" : tasks.getFirst());
        template.add("lastTask", tasks.isEmpty() ? "completed" : tasks.getLast());
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
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
        template.add("packageName", generatedProbesPackage);
        template.add("className", className);
        template.add("processId", processId);
        template.add("eventsTopic", eventsTopic);
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
        template.add("packageName", generatedProbesPackage);
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
        return builder.isEmpty() ? "Unnamed" : builder.toString();
    }

    private static String loadTemplates() {
        return loadTemplateResources(
                "/templates-java/scaffold.stg",
                "/templates-java/scaffold-events.stg",
                "/templates-java/scaffold-subprocess.stg",
                "/templates-java/scaffold-generated-project.stg",
                "/templates-java/scaffold-connect.stg"
        );
    }

    private static String loadTemplateResources(String... resources) {
        StringBuilder templates = new StringBuilder();
        for (String resource : resources) {
            try (InputStream input = BpmnScaffolder.class.getResourceAsStream(resource)) {
                if (input == null) {
                    throw new IllegalStateException("Template not found: " + resource);
                }
                if (!templates.isEmpty()) {
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
        FlowNode target = flow.getTarget();
        String targetId = target != null ? target.getId() : null;
        return new FlowInfo(flow.getId(), targetId, normalized);
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
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(path -> path.getFileName().toString().endsWith(".java"))
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
                    + "            return java.util.concurrent.CompletableFuture.allOf(\n"
                    + "                    " + output.emitter + ".send(msg.getPayload()).toCompletableFuture(),\n"
                    + "                    processEventsEmitter.send(new ProcessEvent(\n"
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
                    + "            ).toJson()).toCompletableFuture()\n"
                    + "            ).thenCompose(ignored -> msg.ack());\n"
                    + "        }\n";
            updated.add(new OutputSpec(output.emitter, output.channel, output.taskId, output.nodeType, output.condition,
                    output.isDefault, block));
        }
        return updated;
    }

    private static String escapeJava(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static long parseRetention(String value) {
        value = value.trim().toLowerCase();

        long multiplier;
        if (value.endsWith("h")) {
            multiplier = 3600_000L;
        } else if (value.endsWith("d")) {
            multiplier = 24 * 3600_000L;
        } else if (value.endsWith("w")) {
            multiplier = 7 * 24 * 3600_000L;
        } else if (value.endsWith("m")) {
            multiplier = 30 * 24 * 3600_000L;
        } else {
            throw new IllegalArgumentException(
                "Invalid retention format: '" + value + "'. Use 42h, 5d, 2w, 1m, or -1.");
        }
        value = value.substring(0, value.length() - 1);
        try {
            return Long.parseLong(value) * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid retention value: '" + value + "'. Use 42h, 5d, 2w, 1m, or -1.");
        }
    }

    private static List<DataStoreConnectSpec> collectDataStoreConnectSpecs(
            String processId,
            List<DataStoreSpec> dataStores,
            List<DataAssociationSpec> dataAssociations
    ) {
        List<DataStoreConnectSpec> specs = new ArrayList<>();
        for (DataStoreSpec store : dataStores) {
            List<String> sourceTopics = new ArrayList<>();
            List<String> sinkTopics = new ArrayList<>();
            for (DataAssociationSpec association : dataAssociations) {
                if (association.taskName == null) {
                    continue;
                }
                if ("input".equals(association.direction) && association.sources.contains(store.name)) {
                    sourceTopics.add(processId + "_" + association.taskName + "_input");
                }
                if ("output".equals(association.direction) && store.name.equals(association.target)) {
                    sinkTopics.add(processId + "_" + association.taskName + "_output");
                }
            }
            if (!sourceTopics.isEmpty()) {
                specs.add(dataStoreConnectSpec(processId, store, "source", distinct(sourceTopics)));
            }
            if (!sinkTopics.isEmpty()) {
                specs.add(dataStoreConnectSpec(processId, store, "sink", distinct(sinkTopics)));
            }
        }
        return specs;
    }

    private static DataStoreConnectSpec dataStoreConnectSpec(
            String processId,
            DataStoreSpec store,
            String mode,
            List<String> topics
    ) {
        String kind = store.kind != null && !store.kind.isBlank() ? store.kind : "unknown";
        String uri = store.uri != null && !store.uri.isBlank() ? store.uri : "<fill in data store URI>";
        String connectorName = processId + "-" + store.name.replace('_', '-') + "-" + mode;
        String topicList = String.join(", ", topics);
        String connectorClass = connectorClassHint(kind, mode);
        String comment = ("source".equals(mode)
                ? "Source connector skeleton for BPMN data store '" + store.name
                + "'. It should write records to the listed Durga task input topic(s)."
                : "Sink connector skeleton for BPMN data store '" + store.name
                + "'. It should read records from the listed Durga task output topic(s).")
                + " Fill in connector-specific settings, credentials, converters, and transforms.";
        return new DataStoreConnectSpec(connectorName, store.name, mode, kind, uri, topicList,
                connectorClass, comment);
    }

    private static String connectorClassHint(String kind, String mode) {
        String normalized = kind.toLowerCase(Locale.ROOT);
        if ("s3".equals(normalized) && "sink".equals(mode)) {
            return "io.confluent.connect.s3.S3SinkConnector";
        }
        if (("postgres".equals(normalized) || "postgresql".equals(normalized) || "jdbc".equals(normalized))
                && "source".equals(mode)) {
            return "io.confluent.connect.jdbc.JdbcSourceConnector";
        }
        if (("postgres".equals(normalized) || "postgresql".equals(normalized) || "jdbc".equals(normalized))
                && "sink".equals(mode)) {
            return "io.confluent.connect.jdbc.JdbcSinkConnector";
        }
        if ("neo4j".equals(normalized) && "sink".equals(mode)) {
            return "streams.kafka.connect.sink.Neo4jSinkConnector";
        }
        return "<fill in connector class>";
    }

    private static List<String> collectTerminalOutputs(Map<String, NodeInfo> nodes) {
        List<String> outputs = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type == NodeType.END) {
                continue;
            }
            for (String targetId : node.outgoingIds) {
                NodeInfo target = nodes.get(targetId);
                if (target != null && target.type == NodeType.END) {
                    outputs.add(node.name + "_output");
                }
            }
        }
        return outputs;
    }

    static class TaskLineage {
        final List<String> reads;
        final List<String> writes;
        final List<String> stores;

        TaskLineage(List<String> reads, List<String> writes, List<String> stores) {
            this.reads = reads != null ? reads : List.of();
            this.writes = writes != null ? writes : List.of();
            this.stores = stores != null ? stores : List.of();
        }
    }

    private static void validateDataObjectSchemas(List<DataObjectSpec> dataObjectSpecs, Path outputRoot) {
        for (DataObjectSpec spec : dataObjectSpecs) {
            if (spec.schema == null || spec.schema.isBlank()) {
                continue;
            }
            Path resourcesDir = outputRoot.resolve("src/main/resources");
            Path schemaPath = SchemaSupport.resolveSchemaRef(spec.schema, resourcesDir);
            if (schemaPath == null) {
                LOG.warn("Data object '{}' references schema '{}' which could not be resolved in '{}'",
                        spec.name, spec.schema, resourcesDir);
                continue;
            }
            List<String> errors = SchemaSupport.validateSchema(schemaPath);
            if (!errors.isEmpty()) {
                LOG.warn("Schema validation issues for '{}' ({}): {}", spec.name, schemaPath, errors);
            } else {
                LOG.info("Data object '{}' schema validated: {}", spec.name, schemaPath);
            }
        }
    }

    private static Map<String, TaskLineage> buildTaskLineage(
            List<TaskSpec> taskSpecs,
            List<DataAssociationSpec> dataAssociationSpecs
    ) {
        Map<String, TaskLineage> lineage = new LinkedHashMap<>();
        Map<String, List<String>> taskReads = new LinkedHashMap<>();
        Map<String, List<String>> taskWrites = new LinkedHashMap<>();
        Map<String, List<String>> taskStores = new LinkedHashMap<>();

        for (DataAssociationSpec assoc : dataAssociationSpecs) {
            String taskName = assoc.taskName;
            if (taskName == null) {
                continue;
            }
            if ("input".equals(assoc.direction)) {
                taskReads.computeIfAbsent(taskName, k -> new ArrayList<>())
                        .addAll(assoc.sources);
            } else {
                taskWrites.computeIfAbsent(taskName, k -> new ArrayList<>())
                        .addAll(assoc.sources);
                if (assoc.target != null && !assoc.target.isBlank()) {
                    taskStores.computeIfAbsent(taskName, k -> new ArrayList<>())
                            .add(assoc.target);
                }
            }
        }

        for (TaskSpec task : taskSpecs) {
            String name = task.name;
            lineage.put(name, new TaskLineage(
                    taskReads.getOrDefault(name, List.of()),
                    taskWrites.getOrDefault(name, List.of()),
                    taskStores.getOrDefault(name, List.of())
            ));
        }
        return lineage;
    }
}
