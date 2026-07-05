package org.gautelis.durga.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Generates a Rust Cargo project from a BPMN model (the {@code --target rust}
 * path). Each plugin task and exclusive gateway becomes a binary under
 * {@code src/bin/}, sharing the runtime in {@code src/lib.rs}. Generated
 * workers depend on the {@code durga-rust} crate and emit the same
 * {@code ProcessEvent} wire format as the Java target, so the Java monitor
 * observes them unchanged.
 *
 * <p>This first target supports plugin ({@link TaskKind#PLUGIN}) tasks and
 * exclusive ({@link NodeType#XOR}) gateways in a start/tasks/gateways/end
 * topology. Other constructs are reported and skipped.
 */
final class RustTargetGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(RustTargetGenerator.class);

    /** Maps a plugin catalog id to its {@code durga_rust::plugins} struct. */
    private static final Map<String, String> RUST_PLUGIN = Map.ofEntries(
            Map.entry("json-transform", "JsonTransform"),
            Map.entry("type-coercer", "TypeCoercion"),
            Map.entry("field-filter", "FieldFilter"),
            Map.entry("string-template", "StringTemplate"),
            Map.entry("mask", "Mask"),
            Map.entry("regex-extract", "RegexExtract"),
            Map.entry("json-flatten", "JsonFlatten"),
            Map.entry("uuid-inject", "UuidInject"),
            Map.entry("timestamp-normalize", "TimestampNormalize"),
            Map.entry("format-detector", "FormatDetector"),
            Map.entry("object-store-collector", "ObjectStoreCollector"),
            Map.entry("object-store-extractor", "ObjectStoreExtractor"),
            Map.entry("json-schema-validator", "JsonSchemaValidator"),
            Map.entry("kv-enricher", "KvEnricher"),
            Map.entry("field-router", "DeadLetterRouter"),
            Map.entry("window-counter", "WindowCounter")
    );

    private RustTargetGenerator() {
    }

    static void generate(ParsedArgs parsed, String processId, Path outputRoot,
                         List<TaskSpec> taskSpecs, Map<String, NodeInfo> nodes,
                         Map<String, List<FlowInfo>> flowsBySource) {
        STGroupString group = new STGroupString(loadTemplate());
        String crateName = processId;
        String crateLib = processId.replace('-', '_');
        String eventsTopic = "process-events-" + processId;
        String durgaRustPath = System.getProperty("durga.rust.crate.path", "../durga-rust");
        Path binDir = outputRoot.resolve("src/bin");

        ST cargo = group.getInstanceOf("cargoToml");
        cargo.add("processId", processId);
        cargo.add("crateName", crateName);
        cargo.add("durgaRustPath", durgaRustPath);
        write(parsed, outputRoot.resolve("Cargo.toml"), cargo.render());

        // Embed the BPMN model in the crate so each worker can self-register it
        // (the Rust counterpart to the Java ModelRegistration bean).
        String bpmnFile = "model.bpmn";
        copyModel(parsed, outputRoot.resolve(bpmnFile));

        ST lib = group.getInstanceOf("libRs");
        lib.add("processId", processId);
        lib.add("eventsTopic", eventsTopic);
        lib.add("bpmnFile", bpmnFile);
        write(parsed, outputRoot.resolve("src/lib.rs"), lib.render());

        ST readme = group.getInstanceOf("readmeMd");
        readme.add("processId", processId);
        readme.add("crateName", crateName);
        write(parsed, outputRoot.resolve("README.md"), readme.render());

        int workers = 0;
        for (TaskSpec task : taskSpecs) {
            if (task.kind != TaskKind.PLUGIN || task.pluginRef == null) {
                LOG.warn("Rust target: skipping task '{}' (kind {} not supported yet)", task.name, task.kind);
                continue;
            }
            String structName = RUST_PLUGIN.get(task.pluginRef);
            if (structName == null) {
                LOG.warn("Rust target: skipping task '{}' (no Rust plugin for '{}')", task.name, task.pluginRef);
                continue;
            }
            NodeInfo taskNode = nodes.get(task.id);
            NodeInfo next = firstTarget(taskNode, nodes, flowsBySource);
            boolean terminal = next == null || next.type == NodeType.END;

            ST bin = group.getInstanceOf("pluginWorkerBin");
            bin.add("processId", processId);
            bin.add("crateLib", crateLib);
            bin.add("activityId", task.name);
            bin.add("structName", structName);
            bin.add("pluginConfig", escapeRust(task.pluginConfig != null ? task.pluginConfig : "."));
            bin.add("inputTopic", inputTopicFor(processId, taskNode, nodes));
            bin.add("outputTopic", terminal ? "" : inputTopicFor(processId, next, nodes));
            bin.add("dlqTopic", processId + "_" + task.name + "_dlq");
            bin.add("groupId", processId + "-" + task.name);
            bin.add("categoryVariant", categoryVariant(task.pluginCategory));
            bin.add("terminal", terminal);
            write(parsed, binDir.resolve(task.name + ".rs"), bin.render());
            workers++;
        }

        for (NodeInfo node : nodes.values()) {
            if (node.type != NodeType.XOR) {
                continue;
            }
            String branches = branchesBlock(processId, node, nodes, flowsBySource);
            ST bin = group.getInstanceOf("gatewayWorkerBin");
            bin.add("processId", processId);
            bin.add("crateLib", crateLib);
            bin.add("activityId", node.name);
            bin.add("inputTopic", inputTopicFor(processId, node, nodes));
            bin.add("groupId", processId + "-" + node.name);
            bin.add("branchesBlock", branches);
            write(parsed, binDir.resolve(node.name + ".rs"), bin.render());
            workers++;
        }

        for (NodeInfo node : nodes.values()) {
            if (node.type == NodeType.AND || node.type == NodeType.OR
                    || node.type == NodeType.SUB_PROCESS || node.type == NodeType.CALL_ACTIVITY
                    || node.type == NodeType.TIMER) {
                LOG.warn("Rust target: BPMN construct '{}' ({}) is not supported yet", node.name, node.type);
            }
        }

        LOG.info("Rust target: generated {} worker(s) for process '{}' in {}", workers, processId, outputRoot);
    }

    private static NodeInfo firstTarget(NodeInfo node, Map<String, NodeInfo> nodes,
                                        Map<String, List<FlowInfo>> flowsBySource) {
        if (node == null) {
            return null;
        }
        List<FlowInfo> flows = flowsBySource.get(node.id);
        if (flows == null || flows.isEmpty()) {
            return null;
        }
        return nodes.get(flows.get(0).targetId);
    }

    private static String branchesBlock(String processId, NodeInfo gateway, Map<String, NodeInfo> nodes,
                                        Map<String, List<FlowInfo>> flowsBySource) {
        List<FlowInfo> flows = flowsBySource.get(gateway.id);
        StringBuilder conditional = new StringBuilder();
        StringBuilder fallback = new StringBuilder();
        if (flows != null) {
            for (FlowInfo flow : flows) {
                NodeInfo target = nodes.get(flow.targetId);
                if (target == null) {
                    continue;
                }
                String topic = inputTopicFor(processId, target, nodes);
                boolean isDefault = flow.id.equals(gateway.defaultFlowId)
                        || flow.condition == null || flow.condition.isBlank();
                if (isDefault) {
                    fallback.append("        (None, \"").append(topic).append("\".to_string()),\n");
                } else {
                    conditional.append("        (Some(\"").append(escapeRust(flow.condition))
                            .append("\".to_string()), \"").append(topic).append("\".to_string()),\n");
                }
            }
        }
        // Conditional branches first, default (if any) last.
        return "vec![\n" + conditional + fallback + "    ]";
    }

    private static String categoryVariant(String category) {
        if (category == null) {
            return "Other";
        }
        return switch (category) {
            case "route" -> "Route";
            case "validate" -> "Validate";
            case "aggregate" -> "Aggregate";
            default -> "Other";
        };
    }

    private static String inputTopic(String processId, String nodeName) {
        return processId + "_" + nodeName + "_in";
    }

    /**
     * Input topic for a node. A task/gateway fed directly by the start event
     * consumes the process start topic ({@code <processId>_start}) so the shared
     * feeder and infrastructure topics line up with the Java target; every other
     * node consumes {@code <processId>_<name>_in}.
     */
    private static String inputTopicFor(String processId, NodeInfo node, Map<String, NodeInfo> nodes) {
        if (node != null) {
            for (String incomingId : node.incomingIds) {
                NodeInfo incoming = nodes.get(incomingId);
                if (incoming != null && incoming.type == NodeType.START) {
                    return processId + "_start";
                }
            }
        }
        return inputTopic(processId, node != null ? node.name : "unknown");
    }

    static String escapeRust(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
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
            LOG.info("[dry-run] would embed BPMN model at {}", target);
            return;
        }
        try {
            java.nio.file.Files.createDirectories(target.getParent());
            java.nio.file.Files.copy(java.nio.file.Path.of(parsed.bpmnPath), target,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to embed BPMN model at " + target, e);
        }
    }

    private static String loadTemplate() {
        try (InputStream input = RustTargetGenerator.class.getResourceAsStream("/templates-rust/scaffold.stg")) {
            if (input == null) {
                throw new IllegalStateException("Rust template not found: /templates-rust/scaffold.stg");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Rust templates", e);
        }
    }
}
