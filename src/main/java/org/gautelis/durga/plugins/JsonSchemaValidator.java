package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import org.gautelis.durga.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
 */
public final class JsonSchemaValidator implements Plugin {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private JsonNode schema;

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        String payloadStr = Plugin.toString(payload);
        String pluginName = "json-schema-validator";
        Counter counter = Metrics.registry().counter("plugin.executions", "plugin", pluginName);
        Timer timer = Metrics.registry().timer("plugin.duration", "plugin", pluginName);
        counter.increment();
        return timer.recordCallable(() -> {
            JsonNode schemaNode = mapper.readTree(config);
            JsonNode input = mapper.readTree(payloadStr);
            String error = validate(input, schemaNode, "$");
            if (error != null) {
                throw new ValidationException(error);
            }
            return payload;
        });
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
                Iterator<Map.Entry<String, JsonNode>> fields = props.properties().iterator();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
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
                    return path + ": value " + node + " below minimum " + schema.get("minimum");
                }
            }
            if (schema.has("maximum")) {
                if (node.asDouble() > schema.get("maximum").asDouble()) {
                    return path + ": value " + node + " above maximum " + schema.get("maximum");
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
                    if (!Pattern.compile(regex).matcher(text).matches()) {
                        return path + ": string '" + text + "' does not match pattern '" + regex + "'";
                    }
                } catch (PatternSyntaxException e) {
                    return path + ": invalid regex pattern '" + regex + "'";
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
        return path + ": value '" + node.asText() + "' not in allowed values " + allowed;
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
