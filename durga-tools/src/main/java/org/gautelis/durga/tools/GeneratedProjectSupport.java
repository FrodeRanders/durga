package org.gautelis.durga.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Helper methods for the non-Java artifact side of scaffolding.
 * <p>
 * This class owns generated configuration, summaries, and README output so the main scaffolder
 * can stay focused on BPMN parsing and code generation.
 */
final class GeneratedProjectSupport {
    private GeneratedProjectSupport() {
    }

    /**
     * Merges generated messaging channels into a scaffolded {@code application.yml}.
     *
     * @param processId process identifier
     * @param tasks generated tasks
     * @param timers generated timers
     * @param messageTopics generated message-event topics
     * @param callActivities generated call activities
     * @param subProcesses generated subprocess scopes
     * @param outputRoot scaffold output root
     */
    static void mergeApplicationYaml(
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageTopics,
            List<String> callActivities,
            List<String> subProcesses,
            Path outputRoot,
            Map<String, String> taskInputChannels,
            List<String> validationTasks
    ) {
        Path outputPath = outputRoot.resolve("src/main/resources/application.yml");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map<String, Object> root = new LinkedHashMap<>();
        if (Files.exists(outputPath)) {
            try {
                root = mapper.readValue(outputPath.toFile(), new TypeReference<>() {
                });
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read existing application.yml: " + outputPath, e);
            }
        }

        ensureBaseRuntimeConfig(root, processId);

        Map<String, Object> mp = ensureMap(root, "mp");
        Map<String, Object> messaging = ensureMap(mp, "messaging");
        Map<String, Object> incoming = ensureMap(messaging, "incoming");
        Map<String, Object> outgoing = ensureMap(messaging, "outgoing");

        incoming.putIfAbsent("process-events-monitor", channelConfig(
                "smallrye-kafka",
                BpmnScaffolder.eventsTopic,
                "org.apache.kafka.common.serialization.StringDeserializer"
        ));

        outgoing.putIfAbsent("process-events", channelConfig(
                "smallrye-kafka",
                BpmnScaffolder.eventsTopic,
                "org.apache.kafka.common.serialization.StringSerializer"
        ));
        outgoing.putIfAbsent("vannak-metadata-events", channelConfig(
                "smallrye-kafka",
                "vannak-metadata-events",
                "org.apache.kafka.common.serialization.StringSerializer"
        ));

        for (String task : tasks) {
            // Incoming channel for this task — unique name mapped to predecessor's output topic
            String incomingChannel = processId + "_" + task + "_in";
            String incomingTopic = taskInputChannels.getOrDefault(task,
                    processId + "_" + task + "_input");
            incoming.putIfAbsent(incomingChannel, channelConfig(
                    "smallrye-kafka",
                    incomingTopic,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));

            // Outgoing channel for this task — unique name mapped to this task's output topic
            String outgoingChannel = processId + "_" + task + "_out";
            String outgoingTopic = processId + "_" + task + "_output";
            outgoing.putIfAbsent(outgoingChannel, channelConfig(
                    "smallrye-kafka",
                    outgoingTopic,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));

            incoming.putIfAbsent(outgoingTopic, channelConfig(
                    "smallrye-kafka",
                    outgoingTopic,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));

            outgoing.putIfAbsent(BpmnScaffolder.inputEmitterChannel(processId, task), channelConfig(
                    "smallrye-kafka",
                    processId + "_" + task + "_input",
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));

            // DLQ channel for this task
            String dlqChannel = processId + "_" + task + "_dlq";
            outgoing.putIfAbsent(dlqChannel, channelConfig(
                    "smallrye-kafka",
                    dlqChannel,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        for (String timer : timers) {
            String incomingKey = processId + "_" + timer + "_input";
            incoming.putIfAbsent(incomingKey, channelConfig(
                    "smallrye-kafka",
                    incomingKey,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(BpmnScaffolder.inputEmitterChannel(processId, timer), channelConfig(
                    "smallrye-kafka",
                    incomingKey,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        for (String messageTopic : messageTopics) {
            incoming.putIfAbsent(messageTopic, channelConfig(
                    "smallrye-kafka",
                    messageTopic,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(messageTopic + "_emit", channelConfig(
                    "smallrye-kafka",
                    messageTopic,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        for (String callActivity : callActivities) {
            String requestTopic = processId + "_" + callActivity + "_call";
            String replyTopic = processId + "_" + callActivity + "_reply";
            String outputTopic = processId + "_" + callActivity + "_output";
            outgoing.putIfAbsent(requestTopic, channelConfig(
                    "smallrye-kafka",
                    requestTopic,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
            incoming.putIfAbsent(replyTopic, channelConfig(
                    "smallrye-kafka",
                    replyTopic,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(replyTopic + "_emit", channelConfig(
                    "smallrye-kafka",
                    replyTopic,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
            incoming.putIfAbsent(outputTopic, channelConfig(
                    "smallrye-kafka",
                    outputTopic,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(outputTopic + "_emit", channelConfig(
                    "smallrye-kafka",
                    outputTopic,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        for (String subProcess : subProcesses) {
            String inputChannel = processId + "_" + subProcess + "_input";
            incoming.putIfAbsent(inputChannel, channelConfig(
                    "smallrye-kafka",
                    inputChannel,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(BpmnScaffolder.inputEmitterChannel(processId, subProcess), channelConfig(
                    "smallrye-kafka",
                    inputChannel,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));

            String outputChannel = processId + "_" + subProcess + "_output";
            incoming.putIfAbsent(outputChannel, channelConfig(
                    "smallrye-kafka",
                    outputChannel,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));
            outgoing.putIfAbsent(outputChannel + "_emit", channelConfig(
                    "smallrye-kafka",
                    outputChannel,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        if (validationTasks != null && !validationTasks.isEmpty()) {
            // Validation-mode shadow workers consume the production input topic through a dedicated
            // consumer group (a separate index) so the production offset is never advanced, and
            // divert their output to a shared topic rather than the task output channel.
            outgoing.putIfAbsent("validation-candidate-outputs", channelConfig(
                    "smallrye-kafka",
                    "validation-candidate-outputs",
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
            for (String task : validationTasks) {
                String validationChannel = processId + "_" + task + "_validation_in";
                String inputTopic = taskInputChannels.getOrDefault(task, processId + "_" + task + "_input");
                String groupId = processId + "_" + task + "_validation";
                incoming.putIfAbsent(validationChannel, validationIncomingConfig(inputTopic, groupId));
            }
        }

        try {
            Files.createDirectories(outputPath.getParent());
            String yaml = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            Files.writeString(outputPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write application.yml", e);
        }
    }

    private static void ensureBaseRuntimeConfig(Map<String, Object> root, String processId) {
        Map<String, Object> kafka = ensureMap(root, "kafka");
        Map<String, Object> bootstrap = ensureMap(kafka, "bootstrap");
        bootstrap.putIfAbsent("servers", "${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}");

        Map<String, Object> quarkus = ensureMap(root, "quarkus");
        Map<String, Object> application = ensureMap(quarkus, "application");
        application.putIfAbsent("name", processId);
        Map<String, Object> http = ensureMap(quarkus, "http");
        http.putIfAbsent("port", "${HTTP_PORT:8080}");
        Map<String, Object> log = ensureMap(quarkus, "log");
        log.putIfAbsent("level", "INFO");
        Map<String, Object> category = ensureMap(log, "category");
        Map<String, Object> gautelis = ensureMap(category, "org.gautelis");
        gautelis.putIfAbsent("level", "DEBUG");
    }

    /**
     * Builds the machine-readable scaffold summary written to {@code summary.json}.
     *
     * @return summary map ready for JSON serialization
     */
    static Map<String, Object> buildSummary(
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageEvents,
            List<String> messageTopics,
            List<String> signalEvents,
            List<String> signalTopics,
            List<String> boundaryEvents,
            List<String> callActivities,
            List<String> subProcesses,
            List<DataObjectSpec> dataObjects,
            List<DataStoreSpec> dataStores,
            List<DataAssociationSpec> dataAssociations,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            List<NodeInfo> ors,
            List<MultiInstanceSpec> multiInstanceSpecs,
            List<String> generatedFiles,
            Map<String, BpmnScaffolder.TaskLineage> taskLineage
    ) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("processId", processId);
        summary.put("tasks", tasks);
        summary.put("timers", timers);
        summary.put("messageEvents", messageEvents);
        summary.put("messageTopics", messageTopics);
        summary.put("signalEvents", signalEvents);
        summary.put("signalTopics", signalTopics);
        summary.put("boundaryEvents", boundaryEvents);
        summary.put("callActivities", callActivities);
        summary.put("subProcesses", subProcesses);
        summary.put("dataObjects", dataObjects.stream().map(GeneratedProjectSupport::dataObjectSummary).toList());
        summary.put("dataStores", dataStores.stream().map(GeneratedProjectSupport::dataStoreSummary).toList());
        summary.put("dataAssociations", dataAssociations.stream().map(GeneratedProjectSupport::dataAssociationSummary).toList());
        if (taskLineage != null && !taskLineage.isEmpty()) {
            List<Map<String, Object>> lineageEntries = new ArrayList<>();
            for (Map.Entry<String, BpmnScaffolder.TaskLineage> entry : taskLineage.entrySet()) {
                Map<String, Object> entryMap = new LinkedHashMap<>();
                entryMap.put("task", entry.getKey());
                entryMap.put("reads", entry.getValue().reads);
                entryMap.put("writes", entry.getValue().writes);
                entryMap.put("stores", entry.getValue().stores);
                lineageEntries.add(entryMap);
            }
            summary.put("taskLineage", lineageEntries);
        }
        summary.put("xorGateways", xors.stream().map(info -> info.name).toList());
        summary.put("orGateways", ors.stream().map(info -> info.name).toList());
        summary.put("andGateways", ands.stream().map(info -> info.name).toList());
        if (!multiInstanceSpecs.isEmpty()) {
            summary.put("multiInstanceTasks", multiInstanceSpecs.stream().map(m -> m.taskName).toList());
        }
        summary.put("generatedFiles", generatedFiles);
        return summary;
    }

    /**
     * Renders the scaffold summary as pretty-printed JSON.
     *
     * @param summary summary map
     * @return JSON text
     */
    static String renderSummaryJson(Map<String, Object> summary) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(summary);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes the generated summary file.
     *
     * @param outputRoot scaffold output root
     * @param summaryJson rendered JSON summary
     */
    static void writeSummary(Path outputRoot, String summaryJson) {
        Path summaryPath = outputRoot.resolve("summary.json");
        try {
            Files.createDirectories(summaryPath.getParent());
            Files.writeString(summaryPath, summaryJson, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write summary.json", e);
        }
    }

    /**
     * Copies the original BPMN model into the generated project's resources directory
     * so it travels with the generated code. The model serves as the source of truth
     * for regeneration and is enriched by the build with custom implementation metadata.
     */
    static void copyBpmnModel(Path outputRoot, String bpmnPath, List<String> generatedFiles) {
        Path source = SafeXml.safePath(bpmnPath);
        if (!Files.exists(source)) {
            System.err.println("Warning: BPMN source file not found for copy: " + source);
            return;
        }
        Path destDir = outputRoot.resolve("src/main/resources");
        Path dest = destDir.resolve(source.getFileName());
        try {
            Files.createDirectories(destDir);
            Files.copy(source, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            generatedFiles.add(outputRoot.relativize(dest).toString());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to copy BPMN model to " + dest, e);
        }
    }

    /**
     * Writes the generated README that explains the scaffolded topology and helper scripts.
     */
    static void writeGeneratedReadme(
            Path outputRoot,
            String processId,
            List<String> tasks,
            List<String> timers,
            List<String> messageEvents,
            List<String> messageTopics,
            List<String> signalEvents,
            List<String> signalTopics,
            List<String> boundaryEvents,
            List<String> callActivities,
            List<String> subProcesses,
            List<DataObjectSpec> dataObjects,
            List<DataStoreSpec> dataStores,
            List<DataAssociationSpec> dataAssociations,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            List<NodeInfo> ors,
            List<MultiInstanceSpec> multiInstanceSpecs,
            List<String> generatedFiles,
            String payloadPreview,
            Map<String, BpmnScaffolder.TaskLineage> taskLineage
    ) {
        Path readmePath = outputRoot.resolve("README.md");
        StringBuilder builder = new StringBuilder();
        builder.append("# Generated BPMN Scaffolding\n\n");
        builder.append("Process: `").append(processId).append("`\n\n");
        builder.append("## Tasks\n");
        for (String task : tasks) {
            builder.append("- `").append(task).append("`");
            BpmnScaffolder.TaskLineage lineage = taskLineage != null ? taskLineage.get(task) : null;
            if (lineage != null && (!lineage.reads.isEmpty() || !lineage.writes.isEmpty() || !lineage.stores.isEmpty())) {
                if (!lineage.reads.isEmpty()) {
                    builder.append(" reads=").append(lineage.reads);
                }
                if (!lineage.writes.isEmpty()) {
                    builder.append(" writes=").append(lineage.writes);
                }
                if (!lineage.stores.isEmpty()) {
                    builder.append(" stores=").append(lineage.stores);
                }
            }
            builder.append("\n");
        }
        if (!timers.isEmpty()) {
            builder.append("\n## Timers\n");
            for (String timer : timers) {
                builder.append("- ").append(timer).append("\n");
            }
        }
        if (!messageEvents.isEmpty()) {
            builder.append("\n## Message Events\n");
            for (String messageEvent : messageEvents) {
                builder.append("- ").append(messageEvent).append("\n");
            }
            builder.append("\nMessage topics:\n");
            for (String topic : messageTopics) {
                builder.append("- ").append(topic).append("\n");
            }
        }
        if (!signalEvents.isEmpty()) {
            builder.append("\n## Signal Events\n");
            for (String signalEvent : signalEvents) {
                builder.append("- ").append(signalEvent).append("\n");
            }
            builder.append("\nSignal topics:\n");
            for (String topic : signalTopics) {
                builder.append("- ").append(topic).append("\n");
            }
        }
        if (!boundaryEvents.isEmpty()) {
            builder.append("\n## Boundary Events\n");
            for (String boundaryEvent : boundaryEvents) {
                builder.append("- ").append(boundaryEvent).append("\n");
            }
        }
        if (!callActivities.isEmpty()) {
            builder.append("\n## Call Activities\n");
            for (String callActivity : callActivities) {
                builder.append("- ").append(callActivity).append("\n");
            }
        }
        if (!subProcesses.isEmpty()) {
            builder.append("\n## Embedded Subprocesses\n");
            for (String subProcess : subProcesses) {
                builder.append("- ").append(subProcess).append(" (generated with explicit subprocess scope handlers)\n");
            }
        }
        if (!dataObjects.isEmpty()) {
            builder.append("\n## Data Objects\n");
            for (DataObjectSpec dataObject : dataObjects) {
                builder.append("- ").append(dataObject.name);
                if (dataObject.schema != null) {
                    builder.append(" schema=").append(dataObject.schema);
                }
                if (dataObject.mediaType != null) {
                    builder.append(" mediaType=").append(dataObject.mediaType);
                }
                if (dataObject.collection) {
                    builder.append(" collection=true");
                }
                builder.append("\n");
            }
        }
        if (!dataStores.isEmpty()) {
            builder.append("\n## Data Stores\n");
            for (DataStoreSpec dataStore : dataStores) {
                builder.append("- ").append(dataStore.name);
                if (dataStore.kind != null) {
                    builder.append(" kind=").append(dataStore.kind);
                }
                if (dataStore.uri != null) {
                    builder.append(" uri=").append(dataStore.uri);
                }
                builder.append("\n");
            }
        }
        if (!dataAssociations.isEmpty()) {
            builder.append("\n## Data Associations\n");
            for (DataAssociationSpec association : dataAssociations) {
                builder.append("- ").append(association.taskName)
                        .append(" ").append(association.direction)
                        .append(" sources=").append(association.sources)
                        .append(" target=").append(association.target)
                        .append("\n");
            }
        }
        builder.append("\n## Gateways\n");
        builder.append("- XOR: ").append(xors.stream().map(info -> info.name).toList()).append("\n");
        builder.append("- OR: ").append(ors.stream().map(info -> info.name).toList()).append("\n");
        builder.append("- AND: ").append(ands.stream().map(info -> info.name).toList()).append("\n");
        builder.append("\n## Generated Files\n");
        for (String file : generatedFiles) {
            builder.append("- ").append(file).append("\n");
        }
        builder.append("\n## Smoke Test Starter\n");
        builder.append("Start a process instance with:\n\n");
        builder.append("```\n");
        builder.append("\n## Run Local Script\n");
        builder.append("Use `run-local.sh` to build and run the starter. Set `START_KAFKA=true` to launch Kafka.\n");
        builder.append("mvn -q clean package\n");
        builder.append("java -cp target/")
                .append(processId)
                .append("-generated-1.0-SNAPSHOT-all.jar org.gautelis.durga.generated.")
                .append(toClassName(processId))
                .append("Starter localhost:9094 valid=true\n");
        builder.append("```\n");
        builder.append("\n## Demo Scenarios\n");
        builder.append("Use `demo-scenario.sh` to publish process-events without a full worker graph.\n\n");
        builder.append("```\n");
        builder.append("./demo-scenario.sh happy\n");
        builder.append("./demo-scenario.sh stuck\n");
        builder.append("./demo-scenario.sh failed\n");
        builder.append("```\n");
        builder.append("\n## Manual Task Input\n");
        builder.append("Use `send-task-input.sh <task-id>` to publish the sample payload for a specific task input topic.\n\n");
        builder.append("```\n");
        builder.append("./send-task-input.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append("\n");
        builder.append("```\n");
        builder.append("\n## External Task Completion\n");
        builder.append("Use `complete-task.sh <task-id> <instance-id>` to advance BPMN user or manual tasks after they have entered a waiting state.\n\n");
        builder.append("```\n");
        builder.append("./complete-task.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append(" <instance-id>\n");
        builder.append("```\n");
        builder.append("\n## External Task Failure\n");
        builder.append("Use `fail-task.sh <task-id> <instance-id>` to emit a failed task event, which is useful for exercising boundary error flows.\n\n");
        builder.append("```\n");
        builder.append("./fail-task.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append(" <instance-id>\n");
        builder.append("```\n");
        builder.append("\n## External Task Escalation\n");
        builder.append("Use `escalate-task.sh <task-id> <instance-id>` to emit an escalated task event for boundary escalation flows.\n\n");
        builder.append("```\n");
        builder.append("./escalate-task.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append(" <instance-id>\n");
        builder.append("```\n");
        if (!callActivities.isEmpty()) {
            builder.append("\n## External Call Activity Completion\n");
            builder.append("Use `complete-call-activity.sh <call-activity-id> <instance-id>` to reply to a generated call-activity wait state.\n\n");
            builder.append("```\n");
            builder.append("./complete-call-activity.sh ").append(callActivities.getFirst()).append(" <instance-id>\n");
            builder.append("```\n");
        }
        if (!messageTopics.isEmpty()) {
            builder.append("\n## External Message Events\n");
            builder.append("Use `send-message-event.sh <message-topic> <instance-id>` to publish a BPMN message into the generated process.\n\n");
            builder.append("```\n");
            builder.append("./send-message-event.sh ").append(messageTopics.getFirst()).append(" <instance-id>\n");
            builder.append("```\n");
        }
        if (!signalTopics.isEmpty()) {
            builder.append("\n## External Signal Events\n");
            builder.append("Use `send-signal-event.sh <signal-topic> <instance-id>` to publish a BPMN signal into the generated process.\n\n");
            builder.append("```\n");
            builder.append("./send-signal-event.sh ").append(signalTopics.getFirst()).append(" <instance-id>\n");
            builder.append("```\n");
        }
        builder.append("\n## Watchers\n");
        builder.append("Use `watch-process-events.sh` to tail the canonical lifecycle topic, or `watch-task-output.sh <task-id>` to inspect one worker output channel.\n\n");
        builder.append("```\n");
        builder.append("./watch-process-events.sh\n");
        builder.append("./watch-process-events.sh <instance-id>\n");
        builder.append("./watch-task-output.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append("\n");
        builder.append("./watch-task-output.sh ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append(" <instance-id>\n");
        builder.append("```\n");
        builder.append("\n## Replay Runbook\n");
        builder.append("Use `replay.sh` to inspect and replay failed records from dead-letter queues or topic offset ranges.\n\n");
        builder.append("```\n");
        builder.append("DURGA_JAR=../durga/target/durga-0.1.0-beta.1.jar ./replay.sh\n");
        builder.append("./replay.sh inspect-dlq ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append("\n");
        builder.append("./replay.sh replay-dlq ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append(" --dry-run\n");
        builder.append("./replay.sh replay-dlq ").append(tasks.isEmpty() ? "task_id" : tasks.getFirst()).append("\n");
        builder.append("```\n");
        builder.append("\n## Example Task Payloads\n");
        builder.append("The generator also writes `task-payloads.json` with sample business-shaped input data per task.\n\n");
        builder.append("```json\n");
        builder.append(payloadPreview);
        builder.append("\n```\n");
        builder.append("\n## Notes\n");
        builder.append("- The generator skips classes that already exist in `src/main/java/`.\n");
        builder.append("- XOR/OR gateway conditions from BPMN conditionExpression are evaluated at runtime.\n");
        builder.append("- Embedded subprocesses generate explicit scope entry/completion handlers.\n");
        builder.append("- Call activities use request/reply topics and generated completion helpers.\n");
        builder.append("- BPMN data objects are modeled as logical data assets, not Kafka topics.\n");
        builder.append("- BPMN data stores describe physical source/sink targets for plugins and connectors.\n");
        builder.append("- `application.yml` was merged with new channels; formatting/comments may change.\n");
        try {
            Files.createDirectories(readmePath.getParent());
            Files.writeString(readmePath, builder.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write generated README", e);
        }
    }

    private static Map<String, Object> dataObjectSummary(DataObjectSpec spec) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", spec.id);
        map.put("name", spec.name);
        putIfNotNull(map, "itemSubjectRef", spec.itemSubjectRef);
        putIfNotNull(map, "structureRef", spec.structureRef);
        putIfNotNull(map, "mediaType", spec.mediaType);
        putIfNotNull(map, "schema", spec.schema);
        map.put("collection", spec.collection);
        return map;
    }

    private static Map<String, Object> dataStoreSummary(DataStoreSpec spec) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", spec.id);
        map.put("name", spec.name);
        putIfNotNull(map, "itemSubjectRef", spec.itemSubjectRef);
        putIfNotNull(map, "structureRef", spec.structureRef);
        putIfNotNull(map, "kind", spec.kind);
        putIfNotNull(map, "uri", spec.uri);
        map.put("unlimited", spec.unlimited);
        return map;
    }

    private static Map<String, Object> dataAssociationSummary(DataAssociationSpec spec) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", spec.id);
        putIfNotNull(map, "taskId", spec.taskId);
        putIfNotNull(map, "taskName", spec.taskName);
        map.put("direction", spec.direction);
        map.put("sources", spec.sources);
        putIfNotNull(map, "target", spec.target);
        putIfNotNull(map, "transformation", spec.transformation);
        return map;
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    static String renderTaskPayloadsPreview(String processId, List<String> tasks) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> payloads = new LinkedHashMap<>();
        payloads.put("processId", processId);
        payloads.put("businessKeyExample", processId + "-demo-001");

        Map<String, Object> tasksPayloads = new LinkedHashMap<>();
        for (String task : tasks) {
            tasksPayloads.put(task, samplePayloadForTask(processId, task));
        }
        payloads.put("tasks", tasksPayloads);
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payloads);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Object> channelConfig(String connector, String topic, String serializerOrDeserializer) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector", connector);
        config.put("topic", topic);
        Map<String, Object> value = new LinkedHashMap<>();
        if (serializerOrDeserializer.contains("Deserializer")) {
            value.put("deserializer", serializerOrDeserializer);
        } else {
            value.put("serializer", serializerOrDeserializer);
        }
        config.put("value", value);
        return config;
    }

    /**
     * Incoming channel for a validation shadow worker: reads the production input topic through a
     * dedicated consumer group so the production offset is untouched. The offset reset defaults to
     * {@code latest} (live concurrent) and is overridable via {@code DURGA_VALIDATION_OFFSET_RESET}
     * (e.g. {@code earliest} for a bounded historic replay near the tail).
     */
    private static Map<String, Object> validationIncomingConfig(String topic, String groupId) {
        Map<String, Object> config = channelConfig(
                "smallrye-kafka", topic, "org.apache.kafka.common.serialization.StringDeserializer");
        Map<String, Object> group = new LinkedHashMap<>();
        group.put("id", groupId);
        config.put("group", group);
        Map<String, Object> auto = new LinkedHashMap<>();
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("reset", "${DURGA_VALIDATION_OFFSET_RESET:latest}");
        auto.put("offset", offset);
        config.put("auto", auto);
        return config;
    }

    private static Map<String, Object> ensureMap(Map<String, Object> root, String key) {
        Object value = root.get(key);
        if (value instanceof Map<?, ?> mapValue) {
            @SuppressWarnings("unchecked")
            Map<String, Object> casted = (Map<String, Object>) mapValue;
            return casted;
        }
        Map<String, Object> created = new LinkedHashMap<>();
        root.put(key, created);
        return created;
    }

    private static Map<String, Object> samplePayloadForTask(String processId, String task) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("processId", processId);
        payload.put("taskId", task);
        payload.put("businessKey", processId + "-demo-001");
        payload.put("correlationId", task + "-corr-001");

        String lowerTask = task.toLowerCase(Locale.ROOT);
        if (lowerTask.contains("invoice")) {
            payload.put("invoiceId", "INV-1001");
            payload.put("amount", 1250);
            payload.put("currency", "EUR");
        }
        if (lowerTask.contains("order")) {
            payload.put("orderId", "ORD-1001");
            payload.put("customerId", "CUST-42");
            payload.put("priority", "normal");
        }
        if (lowerTask.contains("ship")) {
            payload.put("shipmentId", "SHIP-1001");
            payload.put("carrier", "postnord");
        }
        if (lowerTask.contains("review")) {
            payload.put("reviewer", "case.worker");
            payload.put("reviewOutcome", "approved");
        }
        if (lowerTask.contains("approve")) {
            payload.put("approved", true);
        }
        if (lowerTask.contains("reject")) {
            payload.put("approved", false);
            payload.put("rejectionReason", "validation_failed");
        }
        if (lowerTask.contains("valid")) {
            payload.put("valid", true);
        }
        if (lowerTask.contains("notify")) {
            payload.put("notificationChannel", "email");
            payload.put("recipient", "demo@example.com");
        }
        if (payload.size() <= 4) {
            payload.put("exampleValue", task + "-value");
        }
        return payload;
    }

    private static String toClassName(String value) {
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

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unnamed";
        }
        String normalized = value.trim()
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .replaceAll("[^a-zA-Z0-9]+", "_")
                .replaceAll("_+", "_")
                .replaceAll("^_|_$", "")
                .toLowerCase(Locale.ROOT);
        return normalized.isBlank() ? "unnamed" : normalized;
    }
}
