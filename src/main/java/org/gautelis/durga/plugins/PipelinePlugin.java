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

import java.util.Iterator;
import java.util.Map;

/**
 * Contracts and utilities shared by data pipeline plugin executors.
 */
public final class PipelinePlugin {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinePlugin.class);

    static final ObjectMapper MAPPER = new ObjectMapper();

    private PipelinePlugin() {
    }

    /**
     * Reads the named field from a JsonNode, supporting dot-notation paths
     * ({@code "address.city"}) for nested access.
     */
    public static JsonNode fieldAt(JsonNode node, String path) {
        String[] segments = path.split("\\.");
        JsonNode current = node;
        for (String segment : segments) {
            if (current == null || !current.isObject()) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    /**
     * Sets a value at a dot-notation path, creating intermediate objects as needed.
     */
    public static void setFieldAt(ObjectNode root, String path, JsonNode value) {
        String[] segments = path.split("\\.");
        ObjectNode current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            JsonNode child = current.get(segments[i]);
            if (child == null || !child.isObject()) {
                ObjectNode next = MAPPER.createObjectNode();
                current.set(segments[i], next);
                current = next;
            } else {
                current = (ObjectNode) child;
            }
        }
        current.set(segments[segments.length - 1], value);
    }

    /**
     * Records a processing error as a structured JSON record for the dead-letter topic.
     */
    public static String errorRecord(String originalJson, String pluginId, String message) {
        ObjectNode record = MAPPER.createObjectNode();
        record.put("plugin", pluginId);
        record.put("error", message);
        record.put("timestamp", System.currentTimeMillis());
        try {
            record.set("original", MAPPER.readTree(originalJson));
        } catch (JsonProcessingException e) {
            record.put("original", originalJson);
        }
        return record.toString();
    }

    /**
     * Creates a JSON record from a map of string values.
     */
    public static String mapToJson(Map<String, Object> map) {
        ObjectNode node = MAPPER.createObjectNode();
        for (var entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String s) {
                node.put(entry.getKey(), s);
            } else if (value instanceof Number n) {
                node.put(entry.getKey(), n.doubleValue());
            } else if (value instanceof Boolean b) {
                node.put(entry.getKey(), b);
            } else {
                node.put(entry.getKey(), String.valueOf(value));
            }
        }
        return node.toString();
    }

    /**
     * Merge two JsonNodes shallowly — fields in {@code override} take precedence.
     */
    public static ObjectNode shallowMerge(JsonNode base, JsonNode override) {
        ObjectNode result = MAPPER.createObjectNode();
        for (Iterator<Map.Entry<String, JsonNode>> it = base.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            result.set(entry.getKey(), entry.getValue());
        }
        for (Iterator<Map.Entry<String, JsonNode>> it = override.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            result.set(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
