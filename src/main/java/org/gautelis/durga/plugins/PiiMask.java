package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Masks sensitive fields in a JSON payload.
 *
 * <p>Config syntax:
 * <pre>
 * "fields=ssn,email,phone;mask=***;preserve=3"
 * </pre>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code fields} — comma-separated field names (dot-notation for nested)</li>
 *   <li>{@code mask} — replacement character (default {@code *})</li>
 *   <li>{@code preserve} — number of characters to preserve at start and end (default 0)</li>
 * </ul>
 */
public final class PiiMask implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        String fieldsList = null;
        char maskChar = '*';
        int preserve = 0;
        if (config != null && !config.isBlank()) {
            String[] parts = config.split(";");
            for (String part : parts) {
                part = part.trim();
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "fields" -> fieldsList = val;
                        case "mask" -> {
                            if (!val.isEmpty()) maskChar = val.charAt(0);
                        }
                        case "preserve" -> {
                            try { preserve = Integer.parseInt(val); } catch (NumberFormatException ignored) {}
                        }
                    }
                }
            }
        }
        return mask(payload, fieldsList, maskChar, preserve);
    }

    private PiiMask() {
    }

    public static String mask(String json, String fieldsList, char maskChar, int preserve) {
        if (fieldsList == null || fieldsList.isBlank()) {
            return json;
        }
        Set<String> fields = Arrays.stream(fieldsList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        if (fields.isEmpty()) {
            return json;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }

        if (input.isObject()) {
            ObjectNode output = input.deepCopy();
            for (String field : fields) {
                maskField(output, field, maskChar, preserve);
            }
            return output.toString();
        }
        return json;
    }

    private static void maskField(ObjectNode root, String path, char maskChar, int preserve) {
        String[] segments = path.split("\\.");
        JsonNode current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            current = current.get(segments[i]);
            if (current == null || !current.isObject()) {
                return;
            }
        }
        String leaf = segments[segments.length - 1];
        if (!current.has(leaf)) {
            return;
        }
        JsonNode value = current.get(leaf);
        if (value.isTextual()) {
            ((ObjectNode) current).put(leaf, maskText(value.asText(), maskChar, preserve));
        }
    }

    static String maskText(String text, char maskChar, int preserve) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        int len = text.length();
        if (preserve <= 0 || len <= preserve * 2) {
            return String.valueOf(maskChar).repeat(len);
        }
        String prefix = text.substring(0, preserve);
        String suffix = text.substring(len - preserve);
        String middle = String.valueOf(maskChar).repeat(len - preserve * 2);
        return prefix + middle + suffix;
    }
}
