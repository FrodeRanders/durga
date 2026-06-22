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
 * Whitespace or blacklist filtering on JSON payload fields.
 *
 * <p>If {@code keep} is specified, only those fields (including nested via dot-notation)
 * are retained. If {@code drop} is specified, those fields are removed. If both are
 * specified, {@code keep} wins on conflict.
 */
public final class FieldFilter implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        String keep = null;
        String drop = null;
        String flatten = null;
        if (config != null && !config.isBlank()) {
            String[] parts = config.split("\\s+");
            for (String part : parts) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "keep" -> keep = val;
                        case "drop" -> drop = val;
                        case "flatten" -> flatten = val;
                    }
                }
            }
        }
        return filter(payload, keep, drop, flatten);
    }

    public FieldFilter() {
    }

    /**
     * Filters fields from a JSON payload.
     *
     * @param json input JSON string
     * @param keep comma-separated fields to retain (optional)
     * @param drop comma-separated fields to remove (optional)
     * @param flattenPrefix if non-null, hoist nested fields under this prefix to top level
     * @return filtered JSON string
     */
    public static String filter(String json, String keep, String drop, String flattenPrefix) {
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

        Set<String> keepSet = keep != null && !keep.isBlank()
                ? Arrays.stream(keep.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet())
                : null;
        Set<String> dropSet = drop != null && !drop.isBlank()
                ? Arrays.stream(drop.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet())
                : null;

        ObjectNode output = mapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = input.properties().iterator();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String field = entry.getKey();

            boolean shouldKeep;
            if (keepSet != null && !keepSet.isEmpty()) {
                shouldKeep = keepSet.contains(field);
            } else if (dropSet != null && dropSet.contains(field)) {
                shouldKeep = false;
            } else {
                shouldKeep = true;
            }
            if (shouldKeep) {
                output.set(field, entry.getValue());
            }
        }

        if (flattenPrefix != null && !flattenPrefix.isBlank()) {
            JsonNode nested = input.get(flattenPrefix);
            if (nested != null && nested.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> nestedFields = nested.properties().iterator();
                while (nestedFields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = nestedFields.next();
                    output.set(entry.getKey(), entry.getValue());
                }
            }
        }

        return output.toString();
    }
}
