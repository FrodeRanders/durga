package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Detects coarse payload format, datatype, and MIME type.
 */
public final class FormatDetector implements Plugin {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        Detection detection = detect(payload);
        Map<String, String> options = ObjectStoreSupport.parseConfig(config);
        String field = options.getOrDefault("field", "format");
        ObjectNode output = MAPPER.createObjectNode();
        output.set(field, toJson(detection));
        if (Boolean.parseBoolean(options.getOrDefault("includePayload", "false"))) {
            output.put("payload", Plugin.toString(payload));
        }
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Detection is an inspection result, not a replacement payload; the input payload is forwarded
     * unchanged via {@link PluginResult.OutputDisposition#PASSTHROUGH}.
     */
    @Override
    public PluginResult executeWithResult(byte[] payload, String config, PluginExecutionContext context) throws Exception {
        byte[] output = execute(payload, config);
        return PluginResult.passthrough(output, idempotencyKey(payload, config));
    }

    public static Detection detect(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return new Detection("empty", "empty", "application/octet-stream", "none", 0, "");
        }

        String text = tryText(payload);
        if (text == null) {
            return new Detection("binary", "bytes", "application/octet-stream", "binary", payload.length,
                    ObjectStoreSupport.sha256(payload));
        }

        String trimmed = text.stripLeading();
        if (looksLikeJson(trimmed)) {
            try {
                var node = MAPPER.readTree(text);
                String datatype = switch (node.getNodeType()) {
                    case OBJECT -> "object";
                    case ARRAY -> "array";
                    case STRING -> "string";
                    case NUMBER -> node.isIntegralNumber() ? "integer" : "number";
                    case BOOLEAN -> "boolean";
                    case NULL -> "null";
                    default -> "json";
                };
                return new Detection("json", datatype, "application/json", "utf-8", payload.length,
                        ObjectStoreSupport.sha256(payload));
            } catch (IOException ignored) {
                // Fall through to text detection.
            }
        }
        if (looksLikeXml(trimmed)) {
            return new Detection("xml", "document", "application/xml", "utf-8", payload.length,
                    ObjectStoreSupport.sha256(payload));
        }
        if (looksLikeCsv(text)) {
            return new Detection("csv", "table", "text/csv", "utf-8", payload.length,
                    ObjectStoreSupport.sha256(payload));
        }
        return new Detection("text", "string", "text/plain", "utf-8", payload.length,
                ObjectStoreSupport.sha256(payload));
    }

    static ObjectNode toJson(Detection detection) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("format", detection.format());
        node.put("datatype", detection.datatype());
        node.put("mediaType", detection.mediaType());
        node.put("encoding", detection.encoding());
        node.put("bytes", detection.bytes());
        node.put("sha256", detection.sha256());
        return node;
    }

    private static boolean looksLikeJson(String text) {
        return text.startsWith("{") || text.startsWith("[") || text.startsWith("\"")
                || text.startsWith("true") || text.startsWith("false") || text.startsWith("null")
                || text.matches("^-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\s*$");
    }

    private static boolean looksLikeXml(String text) {
        return text.startsWith("<") && text.contains(">");
    }

    private static boolean looksLikeCsv(String text) {
        String[] lines = text.split("\\R", 4);
        if (lines.length < 2) {
            return false;
        }
        int first = commaCount(lines[0]);
        int second = commaCount(lines[1]);
        return first > 0 && first == second;
    }

    private static int commaCount(String value) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == ',') {
                count++;
            }
        }
        return count;
    }

    private static String tryText(byte[] payload) {
        String text = new String(payload, StandardCharsets.UTF_8);
        if (text.indexOf('\uFFFD') >= 0) {
            return null;
        }
        for (byte b : payload) {
            int c = b & 0xff;
            if (c == 0 || (c < 0x09) || (c > 0x0d && c < 0x20)) {
                return null;
            }
        }
        return text;
    }

    public record Detection(
            String format,
            String datatype,
            String mediaType,
            String encoding,
            long bytes,
            String sha256
    ) {
    }
}
