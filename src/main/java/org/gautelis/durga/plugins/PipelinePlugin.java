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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Contracts and utilities shared by data pipeline plugin executors.
 */
public final class PipelinePlugin {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinePlugin.class);

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final int MAX_PATH_LENGTH = 1024;
    static final int MAX_PATH_SEGMENTS = 32;
    static final int MAX_PATH_SEGMENT_LENGTH = 128;
    static final int MAX_REGEX_LENGTH = 512;
    static final int MAX_REGEX_INPUT_LENGTH = 8192;

    private PipelinePlugin() {
    }

    /**
     * Reads the named field from a JsonNode, supporting dot-notation paths
     * ({@code "address.city"}) for nested access.
     */
    public static JsonNode fieldAt(JsonNode node, String path) {
        String[] segments = pathSegments(path);
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
        String[] segments = pathSegments(path);
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
        record.put("error", sanitizeErrorMessage(message));
        record.put("timestamp", System.currentTimeMillis());
        record.put("originalBytes", originalJson != null ? originalJson.getBytes(StandardCharsets.UTF_8).length : 0);
        record.put("originalSha256", sha256(originalJson));
        return record.toString();
    }

    static String[] pathSegments(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Path must not be blank");
        }
        if (path.length() > MAX_PATH_LENGTH) {
            throw new IllegalArgumentException("Path is too long");
        }
        String[] segments = path.split("\\.", -1);
        if (segments.length > MAX_PATH_SEGMENTS) {
            throw new IllegalArgumentException("Path has too many segments");
        }
        for (String segment : segments) {
            if (segment.isBlank()) {
                throw new IllegalArgumentException("Path contains an empty segment");
            }
            if (segment.length() > MAX_PATH_SEGMENT_LENGTH) {
                throw new IllegalArgumentException("Path segment is too long");
            }
        }
        return segments;
    }

    public static Pattern compileSafeRegex(String regex) {
        if (regex == null || regex.isBlank()) {
            throw new IllegalArgumentException("Regex must not be blank");
        }
        if (regex.length() > MAX_REGEX_LENGTH) {
            throw new IllegalArgumentException("Regex is too long");
        }
        if (hasNestedQuantifier(regex)) {
            throw new IllegalArgumentException("Regex contains nested quantifiers");
        }
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid regex pattern", e);
        }
    }

    public static String requireBoundedRegexInput(String text) {
        if (text != null && text.length() > MAX_REGEX_INPUT_LENGTH) {
            throw new IllegalArgumentException("Regex input is too long");
        }
        return text;
    }

    private static boolean hasNestedQuantifier(String regex) {
        return regex.matches(".*\\([^)]*[+*][^)]*\\)[+*?].*")
                || regex.matches(".*\\([^)]*\\{\\d+(,\\d*)?}[^)]*\\)[+*?].*");
    }

    public static String sanitizeErrorMessage(String message) {
        if (message == null || message.isBlank()) {
            return "Plugin execution failed";
        }
        return message.length() <= 512 ? message : message.substring(0, 512);
    }

    private static String sha256(String value) {
        if (value == null) {
            return "";
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
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
        for (Map.Entry<String, JsonNode> entry : base.properties()) {
            result.set(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, JsonNode> entry : override.properties()) {
            result.set(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
