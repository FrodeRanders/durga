package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import org.gautelis.durga.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Normalizes timestamp fields to a consistent format.
 *
 * <p>Config syntax:
 * <pre>
 * "fields=created_at,updated_at;from=epoch_ms;to=ISO8601;zone=UTC"
 * </pre>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code fields} — comma-separated field paths (required)</li>
 *   <li>{@code from} — source format: {@code epoch_s}, {@code epoch_ms}, {@code ISO8601},
 *       {@code RFC3339}, or a custom Java DateTimeFormatter pattern</li>
 *   <li>{@code to} — target format: {@code epoch_s}, {@code epoch_ms}, {@code ISO8601},
 *       {@code RFC3339}, or a custom Java DateTimeFormatter pattern</li>
 *   <li>{@code zone} — timezone for epoch conversion (default UTC)</li>
 *   <li>{@code removeOnError} — if "true", remove the field on parse failure instead of keeping it</li>
 * </ul>
 */
public final class TimestampNormalize implements Plugin {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampNormalize.class);

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        String payloadStr = Plugin.toString(payload);
        String pluginName = "timestamp-normalize";
        Counter counter = Metrics.registry().counter("plugin.executions", "plugin", pluginName);
        Timer timer = Metrics.registry().timer("plugin.duration", "plugin", pluginName);
        counter.increment();
        return timer.recordCallable(() -> {
            String fieldsList = null;
            String from = "epoch_ms";
            String to = "ISO8601";
            String zone = "UTC";
            boolean removeOnError = false;
            if (config != null && !config.isBlank()) {
                String[] parts = config.split(";");
                for (String part : parts) {
                    part = part.trim();
                    int eq = part.indexOf('=');
                    if (eq > 0) {
                        String key = part.substring(0, eq).trim();
                        String val = part.substring(eq + 1).trim();
                        switch (key) {
                            case "fields" -> fieldsList = val;
                            case "from" -> from = val;
                            case "to" -> to = val;
                            case "zone" -> zone = val;
                            case "removeOnError" -> removeOnError = "true".equalsIgnoreCase(val);
                        }
                    }
                }
            }
            return Plugin.toBytes(normalize(payloadStr, fieldsList, from, to, zone, removeOnError));
        });
    }

    private TimestampNormalize() {
    }

    public static String normalize(String json, String fieldsList, String fromFormat,
                                    String toFormat, String zone, boolean removeOnError) {
        if (fieldsList == null || fieldsList.isBlank()) {
            return json;
        }
        Set<String> fields = Arrays.stream(fieldsList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        if (fields.isEmpty()) {
            return json;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }
        if (!input.isObject()) {
            return json;
        }

        ObjectNode output = input.deepCopy();
        ZoneOffset zoneOffset = parseZone(zone);

        for (String field : fields) {
            JsonNode node = PipelinePlugin.fieldAt(output, field);
            if (node == null || !node.isTextual() && !node.isNumber()) {
                continue;
            }
            try {
                Instant instant = parseTimestamp(node, fromFormat, zoneOffset);
                String normalized = formatTimestamp(instant, toFormat, zoneOffset);
                String leafName = field.contains(".") ? field.substring(field.lastIndexOf('.') + 1) : field;
                if (field.contains(".")) {
                    String parentPath = field.substring(0, field.lastIndexOf('.'));
                    JsonNode parent = PipelinePlugin.fieldAt(output, parentPath);
                    if (parent != null && parent.isObject()) {
                        ((ObjectNode) parent).put(leafName, normalized);
                    }
                } else {
                    output.put(leafName, normalized);
                }
            } catch (Exception e) {
                if (removeOnError) {
                    removeField(output, field);
                }
            }
        }
        return output.toString();
    }

    private static Instant parseTimestamp(JsonNode node, String format, ZoneOffset zoneOffset) {
        String text = node.asText().trim();
        return switch (format.toLowerCase()) {
            case "epoch_s" -> Instant.ofEpochSecond(Long.parseLong(text));
            case "epoch_ms" -> Instant.ofEpochMilli(Long.parseLong(text));
            case "iso8601" -> Instant.parse(text);
            case "rfc3339" -> Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(text));
            default -> {
                try {
                    yield Instant.ofEpochMilli(Long.parseLong(text));
                } catch (NumberFormatException e1) {
                    LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatter.ofPattern(format));
                    yield ldt.toInstant(zoneOffset);
                }
            }
        };
    }

    private static String formatTimestamp(Instant instant, String format, ZoneOffset zoneOffset) {
        return switch (format.toLowerCase()) {
            case "epoch_s" -> String.valueOf(instant.getEpochSecond());
            case "epoch_ms" -> String.valueOf(instant.toEpochMilli());
            case "iso8601" -> instant.toString();
            case "rfc3339" -> DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .format(instant.atOffset(zoneOffset));
            default -> {
                LocalDateTime ldt = LocalDateTime.ofInstant(instant, zoneOffset);
                yield ldt.format(DateTimeFormatter.ofPattern(format));
            }
        };
    }

    private static ZoneOffset parseZone(String zone) {
        try {
            return ZoneOffset.of(zone);
        } catch (Exception e) {
            try {
                return java.time.ZoneId.of(zone).getRules().getOffset(Instant.now());
            } catch (Exception e2) {
                return ZoneOffset.UTC;
            }
        }
    }

    private static void removeField(ObjectNode root, String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot < 0) {
            root.remove(path);
            return;
        }
        String parentPath = path.substring(0, lastDot);
        String leaf = path.substring(lastDot + 1);
        JsonNode parent = PipelinePlugin.fieldAt(root, parentPath);
        if (parent != null && parent.isObject()) {
            ((ObjectNode) parent).remove(leaf);
        }
    }
}
