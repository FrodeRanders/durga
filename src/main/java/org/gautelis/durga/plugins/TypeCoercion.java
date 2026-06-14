package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

/**
 * Coerces field values to specified types.
 *
 * <p>Config syntax:
 * <pre>
 * "field1:string, field2:int, field3:long, field4:double, field5:boolean, field6:decimal"
 * </pre>
 */
public final class TypeCoercion implements Plugin {

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        String payloadStr = Plugin.toString(payload);
        return Plugin.toBytes(coerce(payloadStr, config));
    }

    private TypeCoercion() {
    }

    public static String coerce(String json, String expression) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }
        if (expression == null || expression.isBlank() || !input.isObject()) {
            return json;
        }

        ObjectNode output = mapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = input.properties().iterator();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            output.set(entry.getKey(), entry.getValue());
        }

        for (String mapping : expression.split(",")) {
            mapping = mapping.trim();
            if (mapping.isEmpty()) {
                continue;
            }
            int colonIdx = mapping.indexOf(':');
            if (colonIdx < 0) {
                continue;
            }
            String field = mapping.substring(0, colonIdx).trim();
            String type = mapping.substring(colonIdx + 1).trim().toLowerCase();
            JsonNode node = PipelinePlugin.fieldAt(output, field);
            if (node == null || node.isNull()) {
                continue;
            }
            JsonNode coerced = coerceNode(mapper, node, type);
            if (coerced != null) {
                PipelinePlugin.setFieldAt(output, field, coerced);
            }
        }
        return output.toString();
    }

    private static JsonNode coerceNode(ObjectMapper mapper, JsonNode node, String type) {
        switch (type) {
            case "string" -> {
                return mapper.getNodeFactory().textNode(node.asText());
            }
            case "int" -> {
                try {
                    return mapper.getNodeFactory().numberNode(node.asInt());
                } catch (Exception e) {
                    return null;
                }
            }
            case "long" -> {
                try {
                    return mapper.getNodeFactory().numberNode(node.asLong());
                } catch (Exception e) {
                    return null;
                }
            }
            case "double" -> {
                try {
                    return mapper.getNodeFactory().numberNode(node.asDouble());
                } catch (Exception e) {
                    return null;
                }
            }
            case "decimal" -> {
                try {
                    return mapper.getNodeFactory().numberNode(new BigDecimal(node.asText()));
                } catch (Exception e) {
                    return null;
                }
            }
            case "boolean" -> {
                String text = node.asText().trim().toLowerCase();
                if ("true".equals(text) || "1".equals(text) || "yes".equals(text)) {
                    return mapper.getNodeFactory().booleanNode(true);
                }
                if ("false".equals(text) || "0".equals(text) || "no".equals(text)) {
                    return mapper.getNodeFactory().booleanNode(false);
                }
                return mapper.getNodeFactory().booleanNode(node.asBoolean());
            }
            default -> {
                return null;
            }
        }
    }
}
