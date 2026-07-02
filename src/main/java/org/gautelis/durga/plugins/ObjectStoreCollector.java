package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Stores the incoming payload in a local object-store directory and returns a data handle.
 */
public final class ObjectStoreCollector implements Plugin {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        Map<String, String> options = ObjectStoreSupport.parseConfig(config);
        ObjectStoreSupport.StoredObject stored = ObjectStoreSupport.store(payload, options);
        FormatDetector.Detection detection = FormatDetector.detect(payload);
        String assetName = options.getOrDefault("asset", options.getOrDefault("name", "payload"));
        String schema = options.get("schema");
        String handleField = ObjectStoreSupport.handleField(options);

        ObjectNode metadata = MAPPER.createObjectNode();
        metadata.put("bytes", stored.bytes());
        metadata.put("sha256", stored.sha256());
        metadata.put("createdAt", stored.createdAt());
        metadata.put("format", detection.format());
        metadata.put("datatype", detection.datatype());
        metadata.put("encoding", detection.encoding());

        ObjectNode handle = MAPPER.createObjectNode();
        handle.put("name", assetName);
        handle.put("uri", stored.uri());
        handle.put("mediaType", detection.mediaType());
        if (schema != null && !schema.isBlank()) {
            handle.put("schema", schema);
        } else {
            handle.putNull("schema");
        }
        handle.set("metadata", metadata);

        ObjectNode output = MAPPER.createObjectNode();
        output.set(handleField, handle);
        if (Boolean.parseBoolean(options.getOrDefault("includeFormat", "true"))) {
            output.set("format", FormatDetector.toJson(detection));
        }
        if (Boolean.parseBoolean(options.getOrDefault("includeOriginal", "false"))) {
            output.put("payload", new String(payload, StandardCharsets.UTF_8));
        }
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }
}
