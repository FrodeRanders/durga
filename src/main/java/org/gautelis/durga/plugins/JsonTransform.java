package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Applies a dot-notation expression to remap fields from input to output.
 *
 * <p>Expression syntax:
 * <pre>
 * "field1, field2, nested.field3"
 *   → copies those fields from input to output at the same paths
 *
 * "field1:dest1, nested.field2:outer.dest2"
 *   → copies source field to destination path
 *
 * "key:value" (with no input field)
 *   → sets a literal value (numbers auto-detected)
 * </pre>
 */
public final class JsonTransform implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        return transform(payload, config);
    }

    private JsonTransform() {
    }

    /**
     * Transforms input JSON using a mapping expression.
     *
     * @param json       input JSON string
     * @param expression comma-separated field mappings
     * @return transformed JSON string
     * @throws IllegalArgumentException if the expression or JSON is malformed
     */
    public static String transform(String json, String expression) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }
        if (expression == null || expression.isBlank() || expression.equals(".")) {
            return json;
        }

        ObjectNode output = mapper.createObjectNode();
        for (String mapping : expression.split(",")) {
            mapping = mapping.trim();
            if (mapping.isEmpty()) {
                continue;
            }
            int colonIdx = mapping.indexOf(':');
            if (colonIdx >= 0) {
                String left = mapping.substring(0, colonIdx).trim();
                String right = mapping.substring(colonIdx + 1).trim();
                JsonNode sourceValue = PipelinePlugin.fieldAt(input, left);
                if (sourceValue != null) {
                    PipelinePlugin.setFieldAt(output, right, sourceValue);
                } else {
                    JsonNode literalValue = tryParseLiteral(right);
                    if (literalValue != null) {
                        PipelinePlugin.setFieldAt(output, left, literalValue);
                    }
                }
            } else {
                JsonNode sourceValue = PipelinePlugin.fieldAt(input, mapping);
                if (sourceValue != null) {
                    PipelinePlugin.setFieldAt(output, mapping, sourceValue);
                }
            }
        }
        return output.toString();
    }

    private static JsonNode tryParseLiteral(String value) {
        ObjectMapper mapper = new ObjectMapper();
        if ("true".equals(value)) {
            return mapper.getNodeFactory().booleanNode(true);
        }
        if ("false".equals(value)) {
            return mapper.getNodeFactory().booleanNode(false);
        }
        if ("null".equals(value)) {
            return mapper.getNodeFactory().nullNode();
        }
        try {
            long l = Long.parseLong(value);
            return mapper.getNodeFactory().numberNode(l);
        } catch (NumberFormatException e1) {
            try {
                double d = Double.parseDouble(value);
                return mapper.getNodeFactory().numberNode(d);
            } catch (NumberFormatException e2) {
                return mapper.getNodeFactory().textNode(value);
            }
        }
    }
}
