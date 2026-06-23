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

/**
 * Scaffolds a BPMN model into an explicit Durga/Kafka runtime project.
 * <p>
 * The generator favors inspectable source code and helper scripts over a hidden workflow engine,
 * so the generated project can be studied, modified, and demoed locally.
 */
public class BpmnScaffolder {
    private static final Logger LOG = LoggerFactory.getLogger(BpmnScaffolder.class);
    static String generatedPackage = "org.gautelis.durga.generated";
    static String generatedProbesPackage;
    static String eventsTopic;

    public static final String DURGA_VERSION = "0.1.0-beta.1"; // TODO

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
        String yamlPreview = renderYamlPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses);
        String topicsPreview = renderTopicsPreview(group, processId, tasks, allTimers, externalTopics, callActivities, subProcesses, retentionMs);
        String bpmnFileName = bpmnFile.toPath().getFileName().toString();
        String pomPreview = renderPomPreview(group, processId, bpmnFileName, DURGA_VERSION);
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
                dataAssociationSpecs, xors, ands, ors, multiInstanceSpecs, generatedFiles
        );
        String summaryJson = GeneratedProjectSupport.renderSummaryJson(summary);

        if (!parsed.dryRun) {
            GeneratedProjectSupport.writeSummary(outputRoot, summaryJson);
            GeneratedProjectSupport.writeGeneratedReadme(
                    outputRoot, processId, tasks, allTimers, messageEvents, messageTopics, signalEvents,
                    signalTopics, boundaryEvents, callActivities, subProcesses, dataObjectSpecs,
                    dataStoreSpecs, dataAssociationSpecs, xors, ands, ors, multiInstanceSpecs,
                    generatedFiles, payloadPreview
            );
            GeneratedProjectSupport.copyBpmnModel(outputRoot, parsed.bpmnPath, generatedFiles);
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

    private static List<String> combineNames(List<String> primary, List<String> secondary) {
        List<String> combined = new ArrayList<>(primary);
        for (String value : secondary) {
            if (!combined.contains(value)) {
                combined.add(value);
            }
        }
        return combined;
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
        template.add("packageName", generatedProbesPackage);
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
        template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            template.add("packageName", generatedProbesPackage);
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
            long retentionMs
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
        return topicsScript.render();
    }

    private static String renderPomPreview(STGroupString group, String processId, String bpmnFileName, String durgaVersion) {
        ST pom = group.getInstanceOf("pomXml");
        pom.add("processId", processId);
        pom.add("starterClass", toClassName(processId));
        pom.add("bpmnFileName", bpmnFileName);
        pom.add("durgaVersion", durgaVersion);
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
            boolean dryRun
    ) {
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessEvent.java", "processEventClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessState.java", "processStateClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "DataHandle.java", "dataHandleClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessStateStore.java", "processStateStoreClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ScopeCancellationRegistry.java", "scopeCancellationRegistryClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "tools/ModelEnricher.java", "modelEnricherClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/Plugin.java", "pluginInterfaceClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "plugins/PipelinePlugin.java", "pipelinePluginClass");
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
        return builder.length() == 0 ? "Unnamed" : builder.toString();
    }

    private static String loadTemplates() {
        return loadTemplateResources(
                "/templates/scaffold.stg",
                "/templates/scaffold-events.stg",
                "/templates/scaffold-subprocess.stg",
                "/templates/scaffold-generated-project.stg",
                "/templates/scaffold-connect.stg"
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
        return Long.parseLong(value) * multiplier;
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
}
