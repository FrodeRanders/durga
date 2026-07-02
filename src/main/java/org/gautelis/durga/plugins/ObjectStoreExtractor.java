package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Reads a payload from a data handle produced by {@link ObjectStoreCollector}.
 */
public final class ObjectStoreExtractor implements Plugin {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        Map<String, String> options = ObjectStoreSupport.parseConfig(config);
        String uri = resolveUri(payload, ObjectStoreSupport.handleField(options));
        return ObjectStoreSupport.read(uri);
    }

    static String resolveUri(byte[] payload, String handleField) throws Exception {
        String text = Plugin.toString(payload);
        JsonNode node = MAPPER.readTree(text);
        if (node.isTextual()) {
            return node.asText();
        }
        JsonNode handle = PipelinePlugin.fieldAt(node, handleField);
        if (handle == null && node.has("uri")) {
            handle = node;
        }
        if (handle == null || !handle.isObject() || handle.get("uri") == null) {
            throw new IllegalArgumentException("Payload does not contain an object-store handle at '" + handleField + "'");
        }
        return handle.get("uri").asText();
    }
}
