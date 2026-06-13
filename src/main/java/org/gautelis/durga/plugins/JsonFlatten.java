package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Flattens nested JSON to dot-notation keys, or unflattens dot-notation keys
 * back to nested objects.
 *
 * <p>Config syntax:
 * <pre>
 * "direction=flatten;separator=."   (flatten nested to top-level)
 * "direction=unflatten;separator=." (nest dot-notation keys)
 * "direction=flatten;separator=.;maxDepth=3"
 * </pre>
 */
public final class JsonFlatten implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        String direction = "flatten";
        String separator = ".";
        int maxDepth = Integer.MAX_VALUE;
        if (config != null && !config.isBlank()) {
            String[] parts = config.split(";");
            for (String part : parts) {
                part = part.trim();
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "direction" -> direction = val;
                        case "separator" -> separator = val;
                        case "maxDepth" -> {
                            try { maxDepth = Integer.parseInt(val); } catch (NumberFormatException ignored) {}
                        }
                    }
                }
            }
        }
        if ("unflatten".equalsIgnoreCase(direction)) {
            return unflatten(payload, separator);
        }
        return flatten(payload, separator, maxDepth);
    }

    private JsonFlatten() {
    }

    public static String flatten(String json, String separator, int maxDepth) {
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

        ObjectNode output = mapper.createObjectNode();
        flattenNode(mapper, output, "", (ObjectNode) input, separator, 0, maxDepth);
        return output.toString();
    }

    private static void flattenNode(ObjectMapper mapper, ObjectNode output, String prefix,
                                     ObjectNode node, String separator, int depth, int maxDepth) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = prefix.isEmpty() ? entry.getKey() : prefix + separator + entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isObject() && !value.isEmpty() && depth < maxDepth) {
                flattenNode(mapper, output, key, (ObjectNode) value, separator, depth + 1, maxDepth);
            } else if (value.isArray()) {
                output.set(key, value);
            } else {
                output.set(key, value);
            }
        }
    }

    public static String unflatten(String json, String separator) {
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

        ObjectNode output = mapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = input.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            PipelinePlugin.setFieldAt(output, key, value);
        }
        return output.toString();
    }
}
