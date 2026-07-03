package org.gautelis.durga.monitoring;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.BaseElement;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.camunda.bpm.model.bpmn.instance.StartEvent;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperties;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperty;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts {@link AlarmConfig} entries from Camunda extension properties embedded
 * in a BPMN 2.0 model.
 *
 * <h3>Property naming scheme</h3>
 * Properties use the prefix {@code durga:alarm:<alarmId>:<field>}:
 *
 * <table>
 *   <tr><th>Field</th><th>Values</th><th>Required</th></tr>
 *   <tr><td>{@code syndrome}</td><td>{@code HARD_ERROR}, {@code COUNTED}, {@code SLIDING_WINDOW}</td><td>yes</td></tr>
 *   <tr><td>{@code eventType}</td><td>{@code PROCESS_FAILED}, {@code ACTIVITY_ESCALATED}, etc.</td><td>yes</td></tr>
 *   <tr><td>{@code threshold}</td><td>positive integer</td><td>for COUNTED / SLIDING_WINDOW</td></tr>
 *   <tr><td>{@code windowSeconds}</td><td>positive integer</td><td>for SLIDING_WINDOW</td></tr>
 *   <tr><td>{@code severity}</td><td>{@code WARN}, {@code CRITICAL}</td><td>yes</td></tr>
 *   <tr><td>{@code message}</td><td>template with {@code ${processId}}, {@code ${activityId}}, etc.</td><td>yes</td></tr>
 * </table>
 *
 * <h3>Scoping</h3>
 * Where the properties appear determines the alarm scope:
 * <ul>
 *   <li><b>Activity-level:</b> Properties on a {@code ServiceTask} or {@code Task} element.
 *       The alarm applies only to that activity in the enclosing process.</li>
 *   <li><b>Process-level inherited:</b> Properties on the {@code Process} element with
 *       {@code alarmId} prefixed by {@code *}. The parser creates one config per activity
 *       in the process, inheriting the same syndrome, threshold, severity, and message.</li>
 *   <li><b>Process-level aggregate:</b> Properties on the {@code Process} element with
 *       {@code alarmId} prefixed by {@code \$}. Creates a single config with
 *       {@code activityId = null}, counting events from any activity in the process.</li>
 * </ul>
 */
public final class BpmnAlarmConfigParser {
    private static final Logger LOG = LoggerFactory.getLogger(BpmnAlarmConfigParser.class);

    private static final String PROP_PREFIX = "durga:alarm:";
    private static final int PREFIX_LEN = PROP_PREFIX.length();

    private BpmnAlarmConfigParser() {}

    /**
     * Parses a BPMN XML string and extracts all alarm configurations.
     *
     * @param bpmnXml raw BPMN 2.0 XML
     * @return list of {@link AlarmConfig}, may be empty
     */
    public static List<AlarmConfig> parse(String bpmnXml) {
        List<AlarmConfig> configs = new ArrayList<>();
        BpmnModelInstance model;
        try {
            model = Bpmn.readModelFromStream(new ByteArrayInputStream(bpmnXml.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            LOG.warn("Failed to parse BPMN for alarm configs: {}", e.getMessage());
            return configs;
        }

        Process process = model.getModelElementsByType(Process.class).stream().findFirst().orElse(null);
        if (process == null) return configs;

        String processId = normalizeId(process.getId());

        // collect activity-level and process-level properties
        Map<String, Map<String, String>> activityProps = new LinkedHashMap<>(); // activityId → props
        Map<String, String> processProps = new LinkedHashMap<>();

        // process-level properties
        readCamundaProperties(process).forEach((name, value) -> {
            if (name.startsWith(PROP_PREFIX)) processProps.put(name, value);
        });

        // collect all activity IDs (for inherited scope expansion)
        List<String> allActivityIds = new ArrayList<>();

        // activity-level properties
        for (FlowNode node : model.getModelElementsByType(FlowNode.class)) {
            if (node instanceof StartEvent || node instanceof EndEvent) continue;
            String activityId = normalizeId(nameOrId(node.getName(), node.getId()));
            if (activityId.isBlank()) continue;
            allActivityIds.add(activityId);

            Map<String, String> props = new LinkedHashMap<>();
            readCamundaProperties(node).forEach((name, value) -> {
                if (name.startsWith(PROP_PREFIX)) props.put(name, value);
            });
            if (!props.isEmpty()) activityProps.put(activityId, props);
        }

        // 1. parse activity-level configs
        for (var entry : activityProps.entrySet()) {
            String activityId = entry.getKey();
            parsePrefixedProps(entry.getValue(), "", (alarmId, fields) -> {
                AlarmConfig config = buildConfig(alarmId, processId, activityId, fields);
                if (config != null) configs.add(config);
            });
        }

        // 2. parse process-level inherited configs (* prefix)
        parsePrefixedProps(processProps, "*", (alarmId, fields) -> {
            for (String activityId : allActivityIds) {
                AlarmConfig config = buildConfig(alarmId, processId, activityId, fields);
                if (config != null) configs.add(config);
            }
        });

        // 3. parse process-level aggregate configs ($ prefix)
        parsePrefixedProps(processProps, "$", (alarmId, fields) -> {
            AlarmConfig config = buildConfig(alarmId, processId, null, fields);
            if (config != null) configs.add(config);
        });

        return configs;
    }

    /**
     * Groups properties by alarm ID prefix and invokes the handler for each group.
     */
    private static void parsePrefixedProps(Map<String, String> props, String scopePrefix,
                                            java.util.function.BiConsumer<String, Map<String, String>> handler) {
        Map<String, Map<String, String>> groups = new LinkedHashMap<>();

        for (var entry : props.entrySet()) {
            String key = entry.getKey().substring(PREFIX_LEN); // strip "durga:alarm:"
            String value = entry.getValue();

            if (!key.startsWith(scopePrefix)) continue;

            String remainder = key.substring(scopePrefix.length());
            int colon = remainder.indexOf(':');
            if (colon <= 0) continue;

            String alarmId = remainder.substring(0, colon);
            String field = remainder.substring(colon + 1);

            groups.computeIfAbsent(alarmId, k -> new LinkedHashMap<>()).put(field, value);
        }

        for (var group : groups.entrySet()) {
            handler.accept(group.getKey(), group.getValue());
        }
    }

    private static AlarmConfig buildConfig(String alarmId, String processId, String activityId,
                                            Map<String, String> fields) {
        String syndromeStr = fields.get("syndrome");
        String eventTypeStr = fields.get("eventType");
        String thresholdStr = fields.get("threshold");
        String windowStr = fields.get("windowSeconds");
        String severityStr = fields.get("severity");
        String message = fields.get("message");

        if (syndromeStr == null || eventTypeStr == null || severityStr == null || message == null) {
            LOG.debug("Skipping alarm '{}': missing required field (syndrome, eventType, severity, message)", alarmId);
            return null;
        }

        AlarmSyndrome syndrome;
        ProcessEvent.EventType eventType;
        AlarmSeverity severity;
        try {
            syndrome = AlarmSyndrome.valueOf(syndromeStr);
            eventType = ProcessEvent.EventType.valueOf(eventTypeStr);
            severity = AlarmSeverity.valueOf(severityStr);
        } catch (IllegalArgumentException e) {
            LOG.debug("Skipping alarm '{}': invalid enum value", alarmId, e);
            return null;
        }

        int threshold = 0;
        if (syndrome != AlarmSyndrome.HARD_ERROR && thresholdStr != null) {
            try { threshold = Integer.parseInt(thresholdStr); } catch (NumberFormatException e) {
                LOG.debug("Skipping alarm '{}': invalid threshold '{}'", alarmId, thresholdStr);
                return null;
            }
        }

        Duration windowDuration = null;
        if (syndrome == AlarmSyndrome.SLIDING_WINDOW && windowStr != null) {
            try { windowDuration = Duration.ofSeconds(Integer.parseInt(windowStr)); } catch (NumberFormatException e) {
                LOG.debug("Skipping alarm '{}': invalid windowSeconds '{}'", alarmId, windowStr);
                return null;
            }
        }

        // Build a fully qualified config ID: processId:alarmId
        String configId = processId + ":" + alarmId;
        String fullMessage = message
                .replace("${processId}", processId)
                .replace("${activityId}", activityId != null ? activityId : "*");

        try {
            return new AlarmConfig(configId, processId, activityId, eventType, syndrome,
                    threshold, windowDuration, severity, fullMessage);
        } catch (IllegalArgumentException e) {
            LOG.debug("Skipping alarm '{}': {}", alarmId, e.getMessage());
            return null;
        }
    }

    static Map<String, String> readCamundaProperties(BaseElement element) {
        if (element.getExtensionElements() == null) return Map.of();
        CamundaProperties props = element.getExtensionElements()
                .getElementsQuery()
                .filterByType(CamundaProperties.class)
                .singleResult();
        if (props == null || props.getCamundaProperties() == null) return Map.of();
        Map<String, String> values = new LinkedHashMap<>();
        for (CamundaProperty prop : props.getCamundaProperties()) {
            values.put(prop.getCamundaName(), prop.getCamundaValue());
        }
        return values;
    }

    private static String normalizeId(String id) {
        if (id == null || id.isBlank()) return "";
        return id.trim().toLowerCase(java.util.Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
    }

    private static String nameOrId(String name, String id) {
        return name != null && !name.isBlank() ? name : id;
    }
}
