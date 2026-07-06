package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import org.gautelis.durga.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Validates JSON payloads against a JSON Schema-like configuration.
 *
 * <p>Supported validations:
 * <ul>
 * <li>{@code type} --- string, number, integer, boolean, object, array, null</li>
 * <li>{@code required} --- list of required field names</li>
 * <li>{@code properties} --- nested schema validation per field</li>
 * <li>{@code enum} --- whitelist of allowed values</li>
 * <li>{@code minimum} / {@code maximum} --- numeric range</li>
 * <li>{@code minLength} / {@code maxLength} --- string length bounds</li>
 * <li>{@code pattern} --- regex pattern (Java syntax)</li>
 * </ul>
 *
 * <p>The config is a JSON object matching the schema subset above.
 *
 * <p>An optional {@code onInvalid} directive selects how invalid payloads are
 * handled: {@code dlq} (default) routes them to the dead-letter channel,
 * {@code skip} drops them silently, and {@code fail} fails the process. The
 * directive is supplied alongside the schema, e.g.
 * {@code schema={"type":"object"};onInvalid=skip} or, in compact form,
 * {@code required=order_id,amount;onInvalid=fail}.
 */
public final class JsonSchemaValidator implements Plugin {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private JsonNode schema;

    /** How the plugin handles a payload that fails validation. */
    enum OnInvalid {
        DLQ, SKIP, FAIL
    }

    record ValidatorConfig(String schemaConfig, OnInvalid onInvalid) {
    }

    @Override
    public String execute(String payload, String config) throws Exception {
        String pluginName = "json-schema-validator";
        Counter counter = Metrics.registry().counter("plugin.executions", "plugin", pluginName);
        Timer timer = Metrics.registry().timer("plugin.duration", "plugin", pluginName);
        counter.increment();
        return timer.recordCallable(() -> {
            String error = validatePayload(payload, parseConfig(config).schemaConfig());
            if (error != null) {
                throw new ValidationException(error);
            }
            return payload;
        });
    }

    @Override
    public PluginResult executeWithResult(byte[] payload, String config) throws Exception {
        String pluginName = "json-schema-validator";
        Counter counter = Metrics.registry().counter("plugin.executions", "plugin", pluginName);
        Timer timer = Metrics.registry().timer("plugin.duration", "plugin", pluginName);
        counter.increment();
        return timer.recordCallable(() -> {
            ValidatorConfig parsed = parseConfig(config);
            String error = validatePayload(Plugin.toString(payload), parsed.schemaConfig());
            String idempotencyKey = idempotencyKey(payload, config);
            if (error == null) {
                return PluginResult.success(payload, idempotencyKey);
            }
            return switch (parsed.onInvalid()) {
                case SKIP -> PluginResult.skip(idempotencyKey, error);
                case FAIL -> PluginResult.fail(idempotencyKey, error);
                case DLQ -> PluginResult.dlq(payload, idempotencyKey, error);
            };
        });
    }

    private String validatePayload(String payload, String schemaConfig) throws Exception {
        JsonNode input = mapper.readTree(payload);
        if (schemaConfig == null || schemaConfig.isBlank()) {
            // No schema configured: nothing to validate against, treat as valid.
            return null;
        }
        if (!schemaConfig.trim().startsWith("{")) {
            return validateCompactConfig(input, schemaConfig);
        }
        JsonNode schemaNode = mapper.readTree(schemaConfig);
        return validate(input, schemaNode, "$");
    }

    /**
     * Separates an optional {@code onInvalid} directive from the schema
     * configuration. Segments are split on top-level {@code ;} only, so a
     * {@code ;} inside the JSON schema (within braces or string literals) does
     * not act as a separator.
     */
    static ValidatorConfig parseConfig(String config) {
        if (config == null) {
            return new ValidatorConfig(null, OnInvalid.DLQ);
        }
        OnInvalid onInvalid = OnInvalid.DLQ;
        StringBuilder schemaConfig = new StringBuilder();
        for (String segment : splitTopLevel(config)) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String lower = trimmed.toLowerCase(Locale.ROOT);
            if (lower.startsWith("oninvalid=")) {
                onInvalid = parseOnInvalid(trimmed.substring("onInvalid=".length()).trim());
            } else if (lower.startsWith("schema=")) {
                appendSchema(schemaConfig, trimmed.substring("schema=".length()));
            } else {
                appendSchema(schemaConfig, trimmed);
            }
        }
        return new ValidatorConfig(schemaConfig.toString(), onInvalid);
    }

    private static void appendSchema(StringBuilder schemaConfig, String segment) {
        String value = segment.trim();
        if (value.isEmpty()) {
            return;
        }
        if (schemaConfig.length() > 0) {
            schemaConfig.append(';');
        }
        schemaConfig.append(value);
    }

    private static OnInvalid parseOnInvalid(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "skip" -> OnInvalid.SKIP;
            case "fail" -> OnInvalid.FAIL;
            default -> OnInvalid.DLQ;
        };
    }

    private static List<String> splitTopLevel(String config) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < config.length(); i++) {
            char c = config.charAt(i);
            if (inString) {
                current.append(c);
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> {
                    inString = true;
                    current.append(c);
                }
                case '{', '[' -> {
                    depth++;
                    current.append(c);
                }
                case '}', ']' -> {
                    if (depth > 0) {
                        depth--;
                    }
                    current.append(c);
                }
                case ';' -> {
                    if (depth == 0) {
                        parts.add(current.toString());
                        current.setLength(0);
                    } else {
                        current.append(c);
                    }
                }
                default -> current.append(c);
            }
        }
        parts.add(current.toString());
        return parts;
    }

    private String validateCompactConfig(JsonNode input, String config) {
        for (String part : config.split(";")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = part.substring(0, eq).trim();
            String value = part.substring(eq + 1).trim();
            if ("required".equals(key)) {
                for (String field : value.split(",")) {
                    String path = field.trim();
                    if (!path.isEmpty() && PipelinePlugin.fieldAt(input, path) == null) {
                        return "$: missing required field '" + path + "'";
                    }
                }
            }
        }
        return null;
    }

    private String validate(JsonNode node, JsonNode schema, String path) {
        if (node == null || schema == null) {
            return null;
        }

        String type = textOrNull(schema, "type");
        if (type != null) {
            String typeErr = checkType(node, type, path);
            if (typeErr != null) return typeErr;
        }

        if (schema.has("enum")) {
            String enumErr = checkEnum(node, schema.get("enum"), path);
            if (enumErr != null) return enumErr;
        }

        if (type != null && type.equals("object") && node.isObject()) {
            if (schema.has("required")) {
                String reqErr = checkRequired(node, schema.get("required"), path);
                if (reqErr != null) return reqErr;
            }
            if (schema.has("properties")) {
                JsonNode props = schema.get("properties");
                for (Map.Entry<String, JsonNode> entry : props.properties()) {
                    String field = entry.getKey();
                    JsonNode childSchema = entry.getValue();
                    JsonNode child = node.get(field);
                    String childPath = path + "." + field;
                    if (child != null) {
                        String childErr = validate(child, childSchema, childPath);
                        if (childErr != null) return childErr;
                    }
                }
            }
        }

        if (node.isNumber()) {
            if (schema.has("minimum")) {
                if (node.asDouble() < schema.get("minimum").asDouble()) {
                    return path + ": numeric value below minimum " + schema.get("minimum");
                }
            }
            if (schema.has("maximum")) {
                if (node.asDouble() > schema.get("maximum").asDouble()) {
                    return path + ": numeric value above maximum " + schema.get("maximum");
                }
            }
        }

        if (node.isTextual()) {
            String text = node.asText();
            if (schema.has("minLength")) {
                if (text.length() < schema.get("minLength").asInt()) {
                    return path + ": string length " + text.length() + " below minLength " + schema.get("minLength").asInt();
                }
            }
            if (schema.has("maxLength")) {
                if (text.length() > schema.get("maxLength").asInt()) {
                    return path + ": string length " + text.length() + " above maxLength " + schema.get("maxLength").asInt();
                }
            }
            if (schema.has("pattern")) {
                String regex = schema.get("pattern").asText();
                try {
                    String boundedText = PipelinePlugin.requireBoundedRegexInput(text);
                    if (!PipelinePlugin.compileSafeRegex(regex).matcher(boundedText).matches()) {
                        return path + ": string does not match configured pattern";
                    }
                } catch (IllegalArgumentException e) {
                    return path + ": invalid or unsafe regex pattern";
                }
            }
        }

        if (node.isArray() && schema.has("items")) {
            JsonNode itemsSchema = schema.get("items");
            for (int i = 0; i < node.size(); i++) {
                String itemPath = path + "[" + i + "]";
                String itemErr = validate(node.get(i), itemsSchema, itemPath);
                if (itemErr != null) return itemErr;
            }
        }

        return null;
    }

    private static String checkType(JsonNode node, String expected, String path) {
        boolean ok = switch (expected) {
            case "null" -> node.isNull();
            case "string" -> node.isTextual();
            case "number" -> node.isNumber();
            case "integer" -> node.isInt() || node.isLong() || (node.isDouble() && node.asDouble() == Math.floor(node.asDouble()));
            case "boolean" -> node.isBoolean();
            case "object" -> node.isObject();
            case "array" -> node.isArray();
            default -> true;
        };
        return ok ? null : path + ": expected " + expected + " but got " + node.getNodeType();
    }

    private static String checkRequired(JsonNode node, JsonNode required, String path) {
        for (JsonNode field : required) {
            String name = field.asText();
            if (!node.has(name)) {
                return path + ": missing required field '" + name + "'";
            }
        }
        return null;
    }

    private static String checkEnum(JsonNode node, JsonNode allowed, String path) {
        for (JsonNode value : allowed) {
            if (node.equals(value)) {
                return null;
            }
        }
        return path + ": value not in allowed enum";
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null ? value.asText() : null;
    }

    /** Thrown when validation fails; caught by the plugin executor's DLQ handler. */
    public static final class ValidationException extends Exception {
        public ValidationException(String message) {
            super(message);
        }
    }
}
