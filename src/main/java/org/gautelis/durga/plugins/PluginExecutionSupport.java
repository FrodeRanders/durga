package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Shared plugin execution policy used by generated workers.
 */
public final class PluginExecutionSupport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PluginExecutionSupport() {
    }

    public static Result execute(Plugin plugin, byte[] payload, String config) throws Exception {
        Map<String, String> options = ObjectStoreSupport.parseConfig(config);
        String mode = options.getOrDefault("handleMode", options.getOrDefault("dataHandleMode", "payload"));
        String pluginConfig = options.getOrDefault("pluginConfig", config);
        if (!"materialize".equalsIgnoreCase(mode)) {
            PluginResult pluginResult = plugin.executeWithResult(payload, pluginConfig);
            return new Result(pluginResult.output(), payload, false, pluginResult.idempotencyKey(),
                    pluginResult.errorStrategy(), pluginResult.metadata());
        }

        Handle inputHandle = findHandle(payload, ObjectStoreSupport.handleField(options));
        if (inputHandle == null) {
            PluginResult pluginResult = plugin.executeWithResult(payload, pluginConfig);
            return new Result(pluginResult.output(), payload, false, pluginResult.idempotencyKey(),
                    pluginResult.errorStrategy(), pluginResult.metadata());
        }

        byte[] rawInput = ObjectStoreSupport.read(inputHandle.uri());
        PluginResult pluginResult = plugin.executeWithResult(rawInput, pluginConfig);
        byte[] rawOutput = pluginResult.output();
        if (rawOutput == null) {
            rawOutput = rawInput;
        }

        ObjectStoreSupport.StoredObject stored = ObjectStoreSupport.store(rawOutput, options);
        FormatDetector.Detection detection = FormatDetector.detect(rawOutput);
        String assetName = options.getOrDefault("asset", inputHandle.name() != null ? inputHandle.name() : "payload");
        String schema = options.getOrDefault("schema", inputHandle.schema());

        ObjectNode metadata = MAPPER.createObjectNode();
        metadata.put("bytes", stored.bytes());
        metadata.put("sha256", stored.sha256());
        metadata.put("createdAt", stored.createdAt());
        metadata.put("format", detection.format());
        metadata.put("datatype", detection.datatype());
        metadata.put("encoding", detection.encoding());
        if (inputHandle.uri() != null) {
            metadata.put("sourceUri", inputHandle.uri());
        }

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
        output.set(ObjectStoreSupport.handleField(options), handle);
        if (Boolean.parseBoolean(options.getOrDefault("includeFormat", "true"))) {
            output.set("format", FormatDetector.toJson(detection));
        }
        return new Result(output.toString().getBytes(StandardCharsets.UTF_8), rawInput, true,
                pluginResult.idempotencyKey(), pluginResult.errorStrategy(), pluginResult.metadata());
    }

    static Handle findHandle(byte[] payload, String handleField) throws Exception {
        JsonNode node = MAPPER.readTree(Plugin.toString(payload));
        JsonNode handle = PipelinePlugin.fieldAt(node, handleField);
        if (handle == null && node.has("uri")) {
            handle = node;
        }
        if (handle == null || !handle.isObject() || handle.get("uri") == null) {
            return null;
        }
        String name = handle.get("name") != null ? handle.get("name").asText() : null;
        String uri = handle.get("uri").asText();
        String mediaType = handle.get("mediaType") != null ? handle.get("mediaType").asText() : null;
        String schema = handle.get("schema") != null && !handle.get("schema").isNull()
                ? handle.get("schema").asText() : null;
        return new Handle(name, uri, mediaType, schema);
    }

    public record Result(byte[] output, byte[] pluginInput, boolean materializedHandle,
                         String idempotencyKey,
                         PluginResult.ErrorStrategy errorStrategy,
                         java.util.Map<String, Object> metadata) {
    }

    record Handle(String name, String uri, String mediaType, String schema) {
    }
}
