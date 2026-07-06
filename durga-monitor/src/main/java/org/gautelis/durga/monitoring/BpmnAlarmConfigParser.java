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
 *   <tr><td>{@code syndrome}</td><td>{@code HARD_ERROR}, {@code COUNTED}, {@code SLIDING_WINDOW}, {@code STUCK}, {@code CASCADE}, {@code SLA_LATENCY}, {@code SLA_THROUGHPUT}</td><td>yes</td></tr>
 *   <tr><td>{@code eventType}</td><td>{@code PROCESS_FAILED}, {@code ACTIVITY_ESCALATED}, etc.</td><td>event-driven syndromes only (not STUCK / CASCADE / SLA_*)</td></tr>
 *   <tr><td>{@code threshold}</td><td>positive integer</td><td>for COUNTED / SLIDING_WINDOW / CASCADE; minimum calls per window for SLA_THROUGHPUT</td></tr>
 *   <tr><td>{@code windowSeconds}</td><td>positive integer</td><td>window for SLIDING_WINDOW / CASCADE, idle timeout for STUCK, measurement period for SLA_THROUGHPUT</td></tr>
 *   <tr><td>{@code maxLatencyMs}</td><td>positive integer (milliseconds)</td><td>required for SLA_LATENCY: maximum allowed wall-clock duration</td></tr>
 *   <tr><td>{@code severity}</td><td>{@code WARN}, {@code CRITICAL}</td><td>yes</td></tr>
 *   <tr><td>{@code message}</td><td>template with {@code ${processId}}, {@code ${activityId}}, {@code ${latencyMs}}, {@code ${limitMs}}, {@code ${windowSeconds}}, etc.</td><td>yes</td></tr>
 * </table>
 *
 * <h3>SLA scope</h3>
 * SLA syndromes follow the same placement scoping as other alarms. Activity-level properties
 * measure that task (entry&rarr;completion latency, or that task's completion rate); process-level
 * aggregate ({@code $}) properties measure the whole process (start&rarr;completion latency, or the
 * process completion rate); process-level inherited ({@code *}) properties apply the same task-level
 * SLA to every activity.
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
        return parse(bpmnXml, null);
    }

    /**
     * Parses a BPMN XML string and extracts all alarm configurations, using an explicit
     * process id when supplied.
     *
     * @param bpmnXml            raw BPMN 2.0 XML
     * @param processIdOverride  effective process id (e.g. the {@code process-models}
     *                           registration key, which reflects any {@code --process-id}
     *                           override used at scaffold time); when null/blank the model's
     *                           own {@code <process id>} is used
     * @return list of {@link AlarmConfig}, may be empty
     */
    public static List<AlarmConfig> parse(String bpmnXml, String processIdOverride) {
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

        String processId = processIdOverride != null && !processIdOverride.isBlank()
                ? normalizeId(processIdOverride)
                : normalizeId(process.getId());

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
        String maxLatencyStr = fields.get("maxLatencyMs");
        String severityStr = fields.get("severity");
        String message = fields.get("message");

        if (syndromeStr == null || severityStr == null || message == null) {
            LOG.debug("Skipping alarm '{}': missing required field (syndrome, severity, message)", alarmId);
            return null;
        }

        AlarmSyndrome syndrome;
        AlarmSeverity severity;
        try {
            syndrome = AlarmSyndrome.valueOf(syndromeStr);
            severity = AlarmSeverity.valueOf(severityStr);
        } catch (IllegalArgumentException e) {
            LOG.debug("Skipping alarm '{}': invalid enum value", alarmId, e);
            return null;
        }

        // eventType is only meaningful for event-driven syndromes; STUCK / CASCADE ignore it.
        ProcessEvent.EventType eventType = null;
        if (eventTypeStr != null && !eventTypeStr.isBlank()) {
            try {
                eventType = ProcessEvent.EventType.valueOf(eventTypeStr);
            } catch (IllegalArgumentException e) {
                LOG.debug("Skipping alarm '{}': invalid eventType '{}'", alarmId, eventTypeStr);
                return null;
            }
        }
        boolean eventDriven = syndrome == AlarmSyndrome.HARD_ERROR
                || syndrome == AlarmSyndrome.COUNTED
                || syndrome == AlarmSyndrome.SLIDING_WINDOW;
        if (eventDriven && eventType == null) {
            LOG.debug("Skipping alarm '{}': {} requires eventType", alarmId, syndrome);
            return null;
        }

        int threshold = 0;
        if (thresholdStr != null) {
            try { threshold = Integer.parseInt(thresholdStr); } catch (NumberFormatException e) {
                LOG.debug("Skipping alarm '{}': invalid threshold '{}'", alarmId, thresholdStr);
                return null;
            }
        }

        // windowSeconds is the sliding-window size (SLIDING_WINDOW / CASCADE), the idle timeout
        // before an instance is considered stuck (STUCK), or the measurement period (SLA_THROUGHPUT).
        Duration windowDuration = null;
        boolean windowed = syndrome == AlarmSyndrome.SLIDING_WINDOW
                || syndrome == AlarmSyndrome.STUCK
                || syndrome == AlarmSyndrome.CASCADE
                || syndrome == AlarmSyndrome.SLA_THROUGHPUT;
        if (windowed && windowStr != null) {
            try { windowDuration = Duration.ofSeconds(Integer.parseInt(windowStr)); } catch (NumberFormatException e) {
                LOG.debug("Skipping alarm '{}': invalid windowSeconds '{}'", alarmId, windowStr);
                return null;
            }
        }

        // SLA_LATENCY carries the maximum allowed wall-clock duration in windowDuration.
        if (syndrome == AlarmSyndrome.SLA_LATENCY) {
            if (maxLatencyStr == null) {
                LOG.debug("Skipping alarm '{}': SLA_LATENCY requires maxLatencyMs", alarmId);
                return null;
            }
            try { windowDuration = Duration.ofMillis(Long.parseLong(maxLatencyStr)); } catch (NumberFormatException e) {
                LOG.debug("Skipping alarm '{}': invalid maxLatencyMs '{}'", alarmId, maxLatencyStr);
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
                    threshold, windowDuration, severity, fullMessage, AlarmOrigin.EXPLICIT);
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
        return org.gautelis.durga.NameNormalizer.slug(id);
    }

    private static String nameOrId(String name, String id) {
        return name != null && !name.isBlank() ? name : id;
    }
}
