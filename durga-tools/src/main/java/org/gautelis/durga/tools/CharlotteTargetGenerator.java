package org.gautelis.durga.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Generates the first CharlotteOS-oriented process bundle.
 *
 * <p>The target deliberately separates portable activity code from cluster admission. The Rust
 * crate is compileable business-logic scaffolding; the YAML files describe the artifacts,
 * capabilities, and desired placement consumed by Charlotte's signed deployment ingress and node
 * reconcilers. Generated handlers remain fail-closed until developers implement the business
 * behavior; platform readiness and business-code readiness are reported separately.
 */
final class CharlotteTargetGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(CharlotteTargetGenerator.class);
    private static final String API_VERSION = "durga.gautelis.org/charlotte-v1alpha1";
    private static final int ARTIFACT_NAME_CAPACITY = 48;
    private static final int MAX_KAFKA_PRODUCE_ROUTES = 64;
    private static final Set<String> RUST_KEYWORDS = Set.of(
            "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false",
            "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
            "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait",
            "true", "type", "unsafe", "use", "where", "while", "async", "await", "dyn"
    );

    private CharlotteTargetGenerator() {
    }

    static void generate(
            ParsedArgs parsed,
            String processId,
            Path outputRoot,
            List<TaskSpec> taskSpecs,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource,
            List<DataObjectSpec> dataObjects,
            List<DataStoreSpec> dataStores,
            List<DataAssociationSpec> dataAssociations
    ) {
        List<Component> components = components(processId, taskSpecs, nodes, flowsBySource);
        String modelDigest = sha256(Path.of(parsed.bpmnPath));

        writeYaml(parsed, outputRoot.resolve("charlotte/bundle.yaml"),
                bundle(processId, modelDigest, components, nodes, flowsBySource,
                        dataObjects, dataStores, dataAssociations));
        writeYaml(parsed, outputRoot.resolve("charlotte/deployment.yaml"),
                deployment(processId, components));
        writeYaml(parsed, outputRoot.resolve("charlotte/capabilities.yaml"),
                capabilities(processId, components, dataStores));
        write(parsed, outputRoot.resolve("Cargo.toml"), cargoToml(processId));
        write(parsed, outputRoot.resolve("src/lib.rs"), libRs(components));
        for (Component component : components) {
            write(parsed, outputRoot.resolve("src/" + component.moduleName + ".rs"),
                    componentRs(component));
            write(parsed, outputRoot.resolve("charlotte/runtime/src/bin/" + component.moduleName + ".rs"),
                    runtimeAdapterRs(component));
        }
        write(parsed, outputRoot.resolve("charlotte/runtime/Cargo.toml.in"),
                runtimeCargoToml(processId, components));
        write(parsed, outputRoot.resolve("charlotte/build-applications.sh"), runtimeBuildScript());
        write(parsed, outputRoot.resolve("README.md"), readme(processId, components));
        copyModel(parsed, outputRoot.resolve("model.bpmn"));

        LOG.info("Charlotte target: generated {} activity contract(s) and cluster descriptors "
                + "for process '{}' in {}", components.size(), processId, outputRoot);
    }

    private static List<Component> components(
            String processId,
            List<TaskSpec> taskSpecs,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        List<Component> result = new ArrayList<>();
        Set<String> moduleNames = new LinkedHashSet<>();
        for (TaskSpec task : taskSpecs) {
            NodeInfo node = nodes.get(task.id);
            if (node != null) {
                result.add(component(processId, node, task, nodes, flowsBySource, moduleNames));
            }
        }
        nodes.values().stream()
                .filter(node -> node.type == NodeType.XOR)
                .sorted(Comparator.comparing(node -> node.id))
                .forEach(node -> result.add(
                        component(processId, node, null, nodes, flowsBySource, moduleNames)));
        return result;
    }

    private static Component component(
            String processId,
            NodeInfo node,
            TaskSpec task,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource,
            Set<String> moduleNames
    ) {
        String moduleName = uniqueModuleName(rustIdentifier(node.name), node.id, moduleNames);
        String artifactName = artifactName(processId + "-" + node.name);
        String inputTopic = inputTopicFor(processId, node, nodes);
        List<String> outputTopics = outputTopicsFor(processId, node, nodes, flowsBySource);
        String kind = node.type == NodeType.XOR ? "exclusive-gateway" : "activity";
        return new Component(node, task, moduleName, artifactName, kind, inputTopic, outputTopics);
    }

    private static Map<String, Object> bundle(
            String processId,
            String modelDigest,
            List<Component> components,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource,
            List<DataObjectSpec> dataObjects,
            List<DataStoreSpec> dataStores,
            List<DataAssociationSpec> dataAssociations
    ) {
        Map<String, Object> root = resource("CharlotteProcessBundle", processId);
        Map<String, Object> spec = map();
        spec.put("source", Map.of(
                "model", "model.bpmn",
                "mediaType", "application/xml",
                "sha256", modelDigest,
                "semanticIrVersion", "durga-bpmn-v1"
        ));
        spec.put("target", Map.of(
                "os", "CharlotteOS",
                "triple", "aarch64-unknown-none-catten",
                "executionUnit", "one-procedure-artifact-per-activity",
                "runtimeStatus", "contract-scaffold"
        ));
        spec.put("processEventTopic", "process-events-" + processId);

        List<Map<String, Object>> componentEntries = new ArrayList<>();
        for (Component component : components) {
            Map<String, Object> entry = map();
            entry.put("id", component.node.id);
            entry.put("name", component.node.name);
            entry.put("kind", component.kind);
            if (component.task != null) {
                entry.put("taskKind", component.task.kind.bpmnType);
                if (component.task.pluginRef != null) {
                    entry.put("plugin", component.task.pluginRef);
                }
                if (component.task.pluginConfig != null) {
                    entry.put("pluginConfig", component.task.pluginConfig);
                }
            }
            entry.put("sourceModule", "src/" + component.moduleName + ".rs");
            entry.put("artifact", Map.of(
                    "logicalName", component.artifactName,
                    "class", "service",
                    "elf", "artifacts/" + component.artifactName + ".elf"
            ));
            entry.put("inputTopic", component.inputTopic);
            entry.put("outputTopics", component.outputTopics);
            entry.put("runtimeBinding", Map.of(
                    "role", "transactional-step",
                    "procedureEndpoint", component.artifactName,
                    "kafkaProfile", component.node.name + "-step"
            ));
            entry.put("implementationStatus", "handler-not-implemented");
            componentEntries.add(entry);
        }
        spec.put("components", componentEntries);
        spec.put("flows", flows(processId, nodes, flowsBySource));
        spec.put("dataObjects", dataObjects(dataObjects));
        spec.put("dataStores", dataStores(dataStores));
        spec.put("dataAssociations", dataAssociations(dataAssociations));
        spec.put("requiredPlatformContracts", requiredPlatformContracts());
        root.put("spec", spec);
        return root;
    }

    private static Map<String, Object> deployment(String processId, List<Component> components) {
        Map<String, Object> root = resource("CharlotteProcessDeployment", processId);
        Map<String, Object> spec = map();
        spec.put("bundle", Map.of(
                "path", "bundle.yaml",
                "digestSha256", "REQUIRED_AFTER_BUNDLE_FINALIZATION"
        ));
        spec.put("admission", Map.of(
                "requiredSignatureNote", "CLS2",
                "clusterTrustAnchor", "REQUIRED_FROM_CLUSTER_KEY_CEREMONY",
                "provenance", "REQUIRED_AFTER_BUILD"
        ));

        List<Map<String, Object>> deployments = new ArrayList<>();
        for (Component component : components) {
            Map<String, Object> entry = map();
            entry.put("component", component.node.name);
            entry.put("artifact", Map.of(
                    "logicalName", component.artifactName,
                    "path", "../artifacts/" + component.artifactName + ".elf",
                    "sha256", "REQUIRED_AFTER_BUILD",
                    "artifactVersion", 1,
                    "rollbackCounter", 1,
                    "class", "service",
                    "cls2Flags", List.of("no-runtime-code-fetch")
            ));
            entry.put("placement", Map.of(
                    "replicas", 1,
                    "maxInstancesPerNode", 1,
                    "minDistinctNodes", 1,
                    "flags", List.of(),
                    "affinityGroup", 0,
                    "antiAffinityGroup", 0
            ));
            entry.put("currentManifestAdapter", Map.of(
                    "status", "supported-by-signed-deployment-ingress",
                    "objectId", "DERIVED_BY_CHARLOTTE_FROM_LOGICAL_NAME",
                    "nodeKey", 0,
                    "nodeKeyMeaning", "automatic-leader-placement"
            ));
            entry.put("distribution", Map.of(
                    "objectKey", "releases/" + component.artifactName + ".elf",
                    "descriptorPath", "charlotte/descriptors/" + component.artifactName + ".cdep",
                    "transport", "central-s3-compatible-object-store",
                    "notification", "POST /v1/deployments with signed CDEPLOY1",
                    "credentialsVisibleToApplication", false
            ));
            entry.put("transactionalStep", Map.of(
                    "platformArtifact", "kafka-step",
                    "procedureEndpoint", component.artifactName,
                    "profile", component.node.name + "-step",
                    "kafkaConnector", component.artifactName + "-connector-transactional",
                    "implementationStatus", "runner-available; requires-controller-binding"
            ));
            deployments.add(entry);
        }
        spec.put("components", deployments);
        spec.put("rollout", Map.of(
                "applyCommand", deploymentApplyCommand(components),
                "admission", "independent-descriptors",
                "atomic", false,
                "generationFenced", true,
                "strategy", "stop-old-then-activate-new",
                "readinessContract", "REQUIRED_BEFORE_AUTOMATED_ROLLOUT"
        ));
        spec.put("status", Map.of(
                "platformDeployable", true,
                "businessLogicComplete", false,
                "deployable", false,
                "blockedBy", List.of("activity-handlers")
        ));
        root.put("spec", spec);
        return root;
    }

    private static Map<String, Object> capabilities(
            String processId,
            List<Component> components,
            List<DataStoreSpec> dataStores
    ) {
        Map<String, Object> root = resource("CharlotteCapabilityPlan", processId);
        Map<String, Object> spec = map();
        spec.put("principle", "deny-by-default; grant named service connections at launch");
        List<Map<String, Object>> principals = new ArrayList<>();
        for (Component component : components) {
            Map<String, Object> principal = map();
            principal.put("artifact", component.artifactName);
            principal.put("bootstrap", Map.of(
                    "service", "capability-grant-controller",
                    "profile", "signed-CDEPLOY1-read-only",
                    "ambientNameService", false
            ));
            principal.put("grants", List.of(Map.of(
                    "service", component.artifactName,
                    "rights", List.of("publish")
            )));
            principal.put("kafka", Map.of(
                    "granted", false,
                    "reason", "the transactional-step service owns delivery and transaction state"
            ));
            principals.add(principal);
        }
        spec.put("principals", principals);

        List<Map<String, Object>> transactionalSteps = new ArrayList<>();
        for (Component component : components) {
            Map<String, Object> step = map();
            step.put("profile", component.node.name + "-step");
            step.put("role", "transactional-step");
            step.put("procedure", Map.of(
                    "endpoint", component.artifactName,
                    "operation", "invoke",
                    "ownership", "borrow request bytes until reply terminates"
            ));
            step.put("kafka", Map.of(
                    "connector", Map.of(
                            "instance", component.artifactName + "-connector",
                            "authorityEndpoint", Map.of(
                                    "name", component.artifactName + "-connector-transactional",
                                    "rights", List.of("consume", "produce", "transaction")
                            ),
                            "registrationNameSource", "immutable-profile-v6-authority-endpoint",
                            "platformArtifact", "kafka",
                            "profileMaterial", "launcher-only",
                            "grant", "connection-capability-to-transactional-step",
                            "brokerDestinations", Map.of(
                                    "source", "reviewed-capability-profile",
                                    "maxEndpoints", 32,
                                    "selection", "exact-advertised-host-and-port"
                            )
                    ),
                    "profileFormatVersion", 6,
                    "profileDelivery", "read-only-launch-capability",
                    "profileDigest", "sha256-integrity",
                    "maxProduceRoutes", MAX_KAFKA_PRODUCE_ROUTES,
                    "input", Map.of(
                            "topic", component.inputTopic,
                            "partition", 0,
                            "group", processId + "-" + component.node.name,
                            "operations", List.of("fetch")
                    ),
                    "produceRoutes", produceRoutes(processId, component),
                    "transactionalId", processId + "-" + component.node.name + "-${instance}",
                    "operations", List.of(
                            "begin-transaction", "produce", "send-offsets-to-transaction",
                            "commit-transaction", "abort-transaction"
                    ),
                    "profileStatus", "routes-supported; instance-lease-required"
            ));
            transactionalSteps.add(step);
        }
        spec.put("transactionalSteps", transactionalSteps);

        List<Map<String, Object>> stores = new ArrayList<>();
        for (DataStoreSpec store : dataStores) {
            Map<String, Object> entry = map();
            entry.put("id", store.id);
            entry.put("name", store.name);
            entry.put("kind", store.kind != null ? store.kind : "unspecified");
            entry.put("uri", store.uri != null ? store.uri : "REQUIRED");
            entry.put("grantStatus", "requires-explicit-task-assignment");
            if ("s3".equalsIgnoreCase(store.kind)) {
                entry.put("charlotteService", "s3");
                entry.put("credentials", "launch-profile-secret-reference");
                entry.put("operations", List.of("get-object", "put-object"));
            }
            stores.add(entry);
        }
        spec.put("dataStoreProfiles", stores);
        spec.put("ownership", Map.of(
                "runtime", "catten_rt::owned",
                "rule", "owning capabilities are never represented as bare integers",
                "teardown", "RAII locally; explicit consuming close for fallible remote teardown"
        ));
        root.put("spec", spec);
        return root;
    }

    private static List<Map<String, Object>> flows(
            String processId,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, List<FlowInfo>> sourceFlows : flowsBySource.entrySet()) {
            NodeInfo source = nodes.get(sourceFlows.getKey());
            if (source == null) {
                continue;
            }
            for (FlowInfo flow : sourceFlows.getValue()) {
                NodeInfo target = nodes.get(flow.targetId);
                if (target == null) {
                    continue;
                }
                Map<String, Object> entry = map();
                entry.put("id", flow.id);
                entry.put("source", source.id);
                entry.put("target", target.id);
                entry.put("topic", topicForFlow(processId, source, target));
                if (flow.condition != null && !flow.condition.isBlank()) {
                    entry.put("condition", flow.condition);
                }
                result.add(entry);
            }
        }
        return result;
    }

    private static List<Map<String, Object>> produceRoutes(String processId, Component component) {
        if (component.outputTopics.size() + 2 > MAX_KAFKA_PRODUCE_ROUTES) {
            throw new IllegalStateException("Charlotte Kafka profile for '" + component.node.name
                    + "' exceeds " + MAX_KAFKA_PRODUCE_ROUTES
                    + " routes after lifecycle and dead-letter routes are included");
        }
        List<Map<String, Object>> routes = new ArrayList<>();
        int index = 1;
        for (String topic : component.outputTopics) {
            routes.add(Map.of(
                    "index", index++,
                    "topic", topic,
                    "partition", 0,
                    "purpose", "process-output"
            ));
        }
        routes.add(Map.of(
                "index", index++,
                "topic", "process-events-" + processId,
                "partition", 0,
                "purpose", "lifecycle"
        ));
        routes.add(Map.of(
                "index", index,
                "topic", processId + "_" + component.node.name + "_dlq",
                "partition", 0,
                "purpose", "dead-letter"
        ));
        return routes;
    }

    private static List<Map<String, Object>> dataObjects(List<DataObjectSpec> specs) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (DataObjectSpec spec : specs) {
            Map<String, Object> entry = map();
            entry.put("id", spec.id);
            entry.put("name", spec.name);
            putIfPresent(entry, "mediaType", spec.mediaType);
            putIfPresent(entry, "schema", spec.schema);
            putIfPresent(entry, "structureRef", spec.structureRef);
            entry.put("collection", spec.collection);
            result.add(entry);
        }
        return result;
    }

    private static List<Map<String, Object>> dataStores(List<DataStoreSpec> specs) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (DataStoreSpec spec : specs) {
            Map<String, Object> entry = map();
            entry.put("id", spec.id);
            entry.put("name", spec.name);
            putIfPresent(entry, "kind", spec.kind);
            putIfPresent(entry, "uri", spec.uri);
            putIfPresent(entry, "structureRef", spec.structureRef);
            entry.put("unlimited", spec.unlimited);
            result.add(entry);
        }
        return result;
    }

    private static List<Map<String, Object>> dataAssociations(List<DataAssociationSpec> specs) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (DataAssociationSpec spec : specs) {
            Map<String, Object> entry = map();
            entry.put("id", spec.id);
            entry.put("task", spec.taskId);
            entry.put("direction", spec.direction);
            entry.put("sources", spec.sources);
            putIfPresent(entry, "target", spec.target);
            putIfPresent(entry, "transformation", spec.transformation);
            result.add(entry);
        }
        return result;
    }

    private static List<Map<String, Object>> requiredPlatformContracts() {
        return List.of(
                requirement(
                        "kafka-transactional-step-runner",
                        "available-in-charlotte-os",
                        "The bounded kafka_step service owns poll/transaction/offset resources, "
                                + "calls the generated procedure, validates allow-listed outputs, "
                                + "and applies timeout, retry, abort, and DLQ policy."
                ),
                requirement(
                        "capability-grant-controller",
                        "available-in-charlotte-os",
                        "Mint only descriptor-authorized service connections and publication rights; "
                                + "applications receive no ambient name service."
                ),
                requirement(
                        "signed-deployment-reconciler",
                        "available-in-charlotte-os",
                        "Pull signed artifacts through a node-local S3 connector, reconcile multiple "
                                + "48-byte artifact names, and generation-fence retirement."
                ),
                requirement(
                        "placement-controller",
                        "partial",
                        "Automatic leader placement is available for one replica. Capacity, affinity, "
                                + "failure-domain spreading, rescheduling, and replicas greater than one "
                                + "still require cluster-level planning."
                ),
                requirement(
                        "durable-process-state",
                        "design-needed",
                        "Choose Kafka replay, Charlotte object storage, or a replicated state service "
                                + "for joins, timers, and stateful plugins."
                )
        );
    }

    private static Map<String, Object> requirement(String id, String status, String detail) {
        return Map.of("id", id, "status", status, "detail", detail);
    }

    private static String inputTopicFor(String processId, NodeInfo node, Map<String, NodeInfo> nodes) {
        if (node.incomingIds.isEmpty()) {
            return processId + "_" + node.name + "_input";
        }
        NodeInfo source = nodes.get(node.incomingIds.get(0));
        if (source == null) {
            return processId + "_" + node.name + "_input";
        }
        return switch (source.type) {
            case START -> processId + "_start";
            case TASK, CALL_ACTIVITY, SUB_PROCESS -> processId + "_" + source.name + "_output";
            case XOR, AND, OR -> processId + "_" + node.name + "_input";
            default -> processId + "_" + node.name + "_input";
        };
    }

    private static List<String> outputTopicsFor(
            String processId,
            NodeInfo source,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource
    ) {
        List<FlowInfo> flows = flowsBySource.getOrDefault(source.id, List.of());
        LinkedHashSet<String> topics = new LinkedHashSet<>();
        for (FlowInfo flow : flows) {
            NodeInfo target = nodes.get(flow.targetId);
            if (target != null && target.type != NodeType.END) {
                topics.add(topicForFlow(processId, source, target));
            }
        }
        return List.copyOf(topics);
    }

    private static String topicForFlow(String processId, NodeInfo source, NodeInfo target) {
        return switch (source.type) {
            case START -> processId + "_start";
            case XOR, AND, OR -> processId + "_" + target.name + "_input";
            default -> processId + "_" + source.name + "_output";
        };
    }

    private static String cargoToml(String processId) {
        String crateName = processId.toLowerCase(Locale.ROOT).replace('_', '-');
        return "[package]\n"
                + "name = \"" + crateName + "-charlotte-contract\"\n"
                + "version = \"0.1.0\"\n"
                + "edition = \"2024\"\n"
                + "publish = false\n\n"
                + "[lib]\n"
                + "path = \"src/lib.rs\"\n\n"
                + "# This crate is the generated, portable activity contract. The Charlotte runtime\n"
                + "# adapter and ELF bins are intentionally separate until the required platform\n"
                + "# contracts in charlotte/bundle.yaml are implemented.\n";
    }

    private static String libRs(List<Component> components) {
        StringBuilder modules = new StringBuilder("#![no_std]\n\n");
        modules.append("/// Static transport contract generated from BPMN.\n")
                .append("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n")
                .append("pub struct ActivityContract {\n")
                .append("    pub id: &'static str,\n")
                .append("    pub input_topic: &'static str,\n")
                .append("    pub output_topics: &'static [&'static str],\n")
                .append("}\n\n")
                .append("/// Successful decision returned to the transactional-step runner.\n")
                .append("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n")
                .append("pub enum ActivityDecision {\n")
                .append("    Complete,\n")
                .append("    Emit { route: u16, len: usize },\n")
                .append("}\n\n")
                .append("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n")
                .append("pub enum ActivityError {\n")
                .append("    NotImplemented,\n")
                .append("    InvalidPayload,\n")
                .append("}\n\n")
                .append("pub type ActivityResult = Result<ActivityDecision, ActivityError>;\n\n");
        for (Component component : components) {
            modules.append("pub mod ").append(component.moduleName).append(";\n");
        }
        return modules.toString();
    }

    private static String componentRs(Component component) {
        String outputs = component.outputTopics.stream()
                .map(topic -> "\"" + escapeRust(topic) + "\"")
                .reduce((left, right) -> left + ", " + right)
                .orElse("");
        return "use crate::{ActivityContract, ActivityError, ActivityResult};\n\n"
                + "pub const CONTRACT: ActivityContract = ActivityContract {\n"
                + "    id: \"" + escapeRust(component.node.name) + "\",\n"
                + "    input_topic: \"" + escapeRust(component.inputTopic) + "\",\n"
                + "    output_topics: &[" + outputs + "],\n"
                + "};\n\n"
                + "/// Implement the activity without acquiring ambient authority. The Charlotte\n"
                + "/// adapter supplies only capabilities declared in charlotte/capabilities.yaml.\n"
                + "pub fn handle(_payload: &[u8], _output: &mut [u8]) -> ActivityResult {\n"
                + "    Err(ActivityError::NotImplemented)\n"
                + "}\n";
    }

    private static String runtimeCargoToml(String processId, List<Component> components) {
        String packageName = processId.toLowerCase(Locale.ROOT).replace('_', '-')
                + "-charlotte-runtime";
        StringBuilder manifest = new StringBuilder("[package]\n")
                .append("name = \"").append(packageName).append("\"\n")
                .append("version = \"0.1.0\"\n")
                .append("edition = \"2024\"\n")
                .append("publish = false\n\n")
                .append("[dependencies]\n")
                .append("contract = { package = \"")
                .append(processId.toLowerCase(Locale.ROOT).replace('_', '-'))
                .append("-charlotte-contract\", path = \"../..\" }\n")
                .append("catten-rt = { path = \"@CHARLOTTE_ROOT@/crates/catten-rt\" }\n")
                .append("catten-services = { path = \"@CHARLOTTE_ROOT@/crates/catten-services\" }\n")
                .append("charlotte-kafka-step = { path = \"@CHARLOTTE_ROOT@/crates/charlotte-kafka-step\" }\n")
                .append("charlotte-protocol-kafka = { path = \"@CHARLOTTE_ROOT@/crates/charlotte-protocol-kafka\" }\n\n");
        for (Component component : components) {
            manifest.append("[[bin]]\n")
                    .append("name = \"").append(component.artifactName).append("\"\n")
                    .append("path = \"src/bin/").append(component.moduleName).append(".rs\"\n\n");
        }
        return manifest.toString();
    }

    private static String runtimeBuildScript() {
        return "#!/usr/bin/env sh\n"
                + "set -eu\n"
                + ": \"${CHARLOTTE_ROOT:?set CHARLOTTE_ROOT to the CharlotteOS checkout}\"\n"
                + "SCRIPT_DIR=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\n"
                + "RUNTIME_DIR=\"$SCRIPT_DIR/runtime\"\n"
                + "TOOLCHAIN=$(sed -n 's/^channel = \"\\(.*\\)\"/\\1/p' "
                + "\"$CHARLOTTE_ROOT/rust-toolchain.toml\")\n"
                + "RUSTC=$(rustup which --toolchain \"$TOOLCHAIN\" rustc)\n"
                + "export RUSTC\n"
                + "sed \"s|@CHARLOTTE_ROOT@|$CHARLOTTE_ROOT|g\" \"$RUNTIME_DIR/Cargo.toml.in\" "
                + "> \"$RUNTIME_DIR/Cargo.toml\"\n"
                + "mkdir -p \"$RUNTIME_DIR/crates/catten-services\"\n"
                + "cp \"$CHARLOTTE_ROOT/crates/catten-services/link.x\" "
                + "\"$RUNTIME_DIR/crates/catten-services/link.x\"\n"
                + "cd \"$CHARLOTTE_ROOT\"\n"
                + "rustup run \"$TOOLCHAIN\" cargo build --manifest-path \"$RUNTIME_DIR/Cargo.toml\" "
                + "--target \"$CHARLOTTE_ROOT/crates/catten-services/aarch64-unknown-none.json\" "
                + "--release -Z json-target-spec -Z build-std=core,alloc\n";
    }

    private static String runtimeAdapterRs(Component component) {
        return """
                #![no_std]
                #![no_main]

                extern crate alloc;

                use alloc::vec;
                use catten_rt::{Context, owned::{Endpoint, OwnedMemory, ReplyToken}};
                use catten_services::{grant_client, kafka_step as protocol};
                use charlotte_kafka_step::{OutputBatch, OutputRecord};
                use charlotte_protocol_kafka::DeliveredRecord;
                use contract::{ActivityDecision, ActivityError};

                catten_rt::entry!(main);

                const ARTIFACT_NAME: &[u8] = b"%s";

                fn reply_output(reply: ReplyToken, route: u16, value: &[u8]) -> bool {
                    let batch = OutputBatch { records: vec![OutputRecord {
                        route, key: None, value: Some(value),
                    }] };
                    let memory = match OwnedMemory::allocate((value.len() + 4095).div_ceil(4096).max(1)) {
                        Ok(memory) => memory,
                        Err(_) => return false,
                    };
                    let mut mapping = match memory.map_writable() {
                        Ok(mapping) => mapping,
                        Err(_) => return false,
                    };
                    let len = match batch.encode(mapping.as_mut_slice()) {
                        Ok(len) => len,
                        Err(_) => return false,
                    };
                    let memory = match mapping.unmap() {
                        Ok(memory) => memory,
                        Err(_) => return false,
                    };
                    reply.reply_move(memory, len as i64).is_ok()
                }

                fn main(ctx: Context) -> ! {
                    let bootstrap = ctx.bootstrap_connection().unwrap_or_else(|| catten_rt::domain_abort());
                    let descriptor = ctx.profile_memory().unwrap_or_else(|| catten_rt::domain_abort());
                    let endpoint = Endpoint::create(protocol::INTERFACE, protocol::VERSION, 8)
                        .unwrap_or_else(|_| catten_rt::domain_abort());
                    if grant_client::publish(bootstrap, &descriptor, ARTIFACT_NAME, &endpoint)
                        .is_err()
                    {
                        catten_rt::domain_abort();
                    }
                    loop {
                        let mut message = endpoint.receive().unwrap_or_else(|_| catten_rt::domain_abort());
                        let Some(reply) = message.reply.take() else { continue; };
                        if message.opcode != protocol::OP_INVOKE {
                            let _ = reply.reply(protocol::RESULT_INVALID);
                            continue;
                        }
                        let Some(memory) = message.memory.take() else {
                            let _ = reply.reply(protocol::RESULT_INVALID);
                            continue;
                        };
                        let mapping = match memory.map_read_only() {
                            Ok(mapping) => mapping,
                            Err(_) => {
                                let _ = reply.reply(protocol::RESULT_INVALID);
                                continue;
                            }
                        };
                        let Some(record) = DeliveredRecord::decode(mapping.as_slice()) else {
                            let _ = reply.reply(protocol::RESULT_INVALID);
                            continue;
                        };
                        let mut output = vec![0u8; 65_535];
                        let decision = contract::%s::handle(
                            record.value.unwrap_or_default(), &mut output
                        );
                        drop(mapping);
                        match decision {
                            Ok(ActivityDecision::Complete) => { let _ = reply.reply(0); }
                            Ok(ActivityDecision::Emit { route, len }) if len <= output.len() => {
                                if !reply_output(reply, route, &output[..len]) {
                                    catten_rt::domain_abort();
                                }
                            }
                            Ok(ActivityDecision::Emit { .. }) | Err(ActivityError::InvalidPayload) => {
                                let _ = reply.reply(protocol::RESULT_INVALID);
                            }
                            Err(ActivityError::NotImplemented) => {
                                let _ = reply.reply(protocol::RESULT_TERMINAL);
                            }
                        }
                    }
                }
                """.formatted(escapeRust(component.artifactName), component.moduleName);
    }

    private static String deploymentApplyCommand(List<Component> components) {
        return "cluster-sign deployment-apply 127.0.0.1:8081 120 "
                + components.stream()
                .map(component -> "charlotte/descriptors/" + component.artifactName + ".cdep")
                .collect(java.util.stream.Collectors.joining(" "));
    }

    private static String readme(String processId, List<Component> components) {
        return "# " + processId + " — CharlotteOS target\n\n"
                + "This directory is an initial CharlotteOS process bundle generated from `model.bpmn`. "
                + "It contains Charlotte application source contracts plus an actionable platform "
                + "deployment plan. The generated handlers deliberately fail closed until their business "
                + "logic is implemented.\n\n"
                + "The generated Rust crate contains `no_std` activity contracts and fail-closed "
                + "handler stubs. Implement each `handle` function as a procedure endpoint. The "
                + "generic transactional-step service owns Kafka delivery and transaction resources "
                + "and calls that procedure; generated business code does not receive Kafka authority. "
                + "Any other reviewed launch capabilities must be owned through `catten_rt::owned`. "
                + "Do not store owning capabilities as integers.\n\n"
                + "## Descriptors\n\n"
                + "- `charlotte/bundle.yaml` preserves the BPMN digest, component graph, topics, data "
                + "assets, and platform requirements.\n"
                + "- `charlotte/deployment.yaml` records CLS2 admission metadata and the exact "
                + "`PlacementPolicy` fields. A zero node key requests Charlotte's current automatic "
                + "single-replica placement; replica spreading still needs a cluster scheduler.\n"
                + "- `charlotte/capabilities.yaml` is a least-authority review plan consumed by the "
                + "capability-grant controller. Application bootstrap contains that controller and a "
                + "read-only signed descriptor, never the ambient name service.\n\n"
                + "## Platform integration status\n\n"
                + "Charlotte's low-level Kafka profile supports one consume route and allow-listed "
                + "multi-topic produce routes in one transaction. Its version-6 profile carries "
                + "separately named authority endpoints with enforced Kafka-rights ceilings, at most "
                + "32 reviewed broker destinations and 64 routes, is protected by a SHA-256 integrity "
                + "digest, uses bounded extensible authentication sections, and is delivered as a "
                + "read-only launch capability. "
                + "CharlotteOS now provides the higher-level `kafka_step` runner and its bounded "
                + "procedure ABI, output validation, timeout/retry policy, and transactional DLQ path. "
                + "Charlotte's signed deployment ingress, multi-application node reconciler, and grant "
                + "controller now satisfy the platform side of the generated plan. Deployment remains "
                + "blocked only while generated activity handlers return `NotImplemented`.\n\n"
                + "Build the portable contract with `cargo test`. Building deployable AArch64 ELFs, "
                + "signing CLS2 notes, computing provenance/digests, granting capabilities, and "
                + "uploading signed ELFs to the central S3-compatible store remain release-pipeline "
                + "stages because they require cluster-owned keys and endpoints. Write each signed "
                + "descriptor to the `descriptorPath` in `charlotte/deployment.yaml`, then submit and "
                + "observe the component set with:\n\n```text\n"
                + deploymentApplyCommand(components) + "\n```\n\n"
                + "This prevalidates the whole local descriptor set and waits for generation-fenced "
                + "readiness, but Charlotte currently commits each descriptor independently. A partial "
                + "failure is safe to retry with the exact same descriptors; atomic process-bundle "
                + "admission and coordinated rollback remain cluster-controller work.\n";
    }

    private static Map<String, Object> resource(String kind, String name) {
        Map<String, Object> root = map();
        root.put("apiVersion", API_VERSION);
        root.put("kind", kind);
        root.put("metadata", Map.of("name", name));
        return root;
    }

    private static Map<String, Object> map() {
        return new LinkedHashMap<>();
    }

    private static void putIfPresent(Map<String, Object> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }

    private static String artifactName(String raw) {
        String normalized = raw.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_-]+", "-")
                .replaceAll("^-+|-+$", "");
        if (normalized.isBlank()) {
            normalized = "durga-activity";
        }
        if (normalized.length() <= ARTIFACT_NAME_CAPACITY) {
            return normalized;
        }
        String suffix = digest(raw.getBytes(StandardCharsets.UTF_8)).substring(0, 8);
        return normalized.substring(0, ARTIFACT_NAME_CAPACITY - suffix.length() - 1) + "-" + suffix;
    }

    private static String rustIdentifier(String raw) {
        String identifier = raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        if (identifier.isBlank()) {
            identifier = "activity";
        }
        if (Character.isDigit(identifier.charAt(0)) || RUST_KEYWORDS.contains(identifier)) {
            identifier = "activity_" + identifier;
        }
        return identifier;
    }

    private static String uniqueModuleName(String candidate, String nodeId, Set<String> names) {
        if (names.add(candidate)) {
            return candidate;
        }
        String unique = candidate + "_" + digest(nodeId.getBytes(StandardCharsets.UTF_8)).substring(0, 8);
        names.add(unique);
        return unique;
    }

    private static String escapeRust(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String sha256(Path path) {
        try {
            return digest(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to hash BPMN model " + path, e);
        }
    }

    private static String digest(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void writeYaml(ParsedArgs parsed, Path path, Map<String, Object> document) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
            write(parsed, path, mapper.writeValueAsString(document));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to render " + path, e);
        }
    }

    private static void write(ParsedArgs parsed, Path path, String content) {
        if (parsed.dryRun) {
            LOG.info("[dry-run] would write {}", path);
            return;
        }
        BpmnScaffolder.writeFile(path, content);
    }

    private static void copyModel(ParsedArgs parsed, Path target) {
        if (parsed.dryRun) {
            LOG.info("[dry-run] would copy BPMN model to {}", target);
            return;
        }
        try {
            Files.createDirectories(target.getParent());
            Files.copy(Path.of(parsed.bpmnPath), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to copy BPMN model to " + target, e);
        }
    }

    private record Component(
            NodeInfo node,
            TaskSpec task,
            String moduleName,
            String artifactName,
            String kind,
            String inputTopic,
            List<String> outputTopics
    ) {
    }
}
