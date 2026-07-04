package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enriches a JSON payload by looking up external data and merging it in.
 *
 * <p>Supports two source modes:
 * <ul>
 * <li>{@code inline} — data is provided directly in configuration as a map of key to
 *     JSON objects</li>
 * <li>{@code kafka_topic} — (future) data is looked up from a compacted Kafka topic</li>
 * </ul>
 *
 * <p>The enrichment is keyed by the value of {@code keyField} in the input payload.
 */
public final class KvEnricher implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        String keyField = "_id";
        java.util.Map<String, String> inline = new java.util.LinkedHashMap<>();
        if (config != null && !config.isBlank()) {
            String[] parts = config.split(";");
            for (String part : parts) {
                int eq = part.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String key = part.substring(0, eq).trim();
                String val = part.substring(eq + 1).trim();
                switch (key) {
                    case "keyField" -> keyField = val;
                    case "inline" -> {
                        if (val.startsWith("{") && val.endsWith("}")) {
                            inline.putAll(parseInlineMap(val.substring(1, val.length() - 1)));
                        }
                    }
                }
            }
        }
        KvEnricher enricher = new KvEnricher(keyField, inline);
        return enricher.enrich(payload);
    }

    /**
     * Parses the inline map body ({@code key:{json}, key:{json}}) into a map of key to raw
     * enrichment JSON. Splitting is brace/bracket/quote aware, so commas and colons inside a
     * value's JSON object do not break the entry boundaries.
     */
    static Map<String, String> parseInlineMap(String body) {
        Map<String, String> map = new java.util.LinkedHashMap<>();
        int depth = 0;
        boolean inString = false;
        char quote = 0;
        int entryStart = 0;
        int colon = -1;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (inString) {
                if (c == quote && (i == 0 || body.charAt(i - 1) != '\\')) {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"', '\'' -> { inString = true; quote = c; }
                case '{', '[' -> depth++;
                case '}', ']' -> depth--;
                case ':' -> { if (depth == 0 && colon < 0) colon = i; }
                case ',' -> {
                    if (depth == 0) {
                        addInlineEntry(map, body, entryStart, colon, i);
                        entryStart = i + 1;
                        colon = -1;
                    }
                }
                default -> { }
            }
        }
        addInlineEntry(map, body, entryStart, colon, body.length());
        return map;
    }

    private static void addInlineEntry(Map<String, String> map, String body, int start, int colon, int end) {
        if (colon < 0 || colon <= start) {
            return;
        }
        String key = body.substring(start, colon).trim();
        String value = body.substring(colon + 1, end).trim();
        if (!key.isEmpty() && !value.isEmpty()) {
            map.put(key, value);
        }
    }

    private final String keyField;
    private final Map<String, String> inlineData;

    public KvEnricher() {
        this("_id", Map.of());
    }

    public KvEnricher(String keyField, Map<String, String> inlineData) {
        this.keyField = keyField;
        this.inlineData = inlineData != null
                ? new ConcurrentHashMap<>(inlineData)
                : new ConcurrentHashMap<>();
    }

    /**
     * Enriches a JSON payload by looking up the key field value in the inline data store
     * and shallow-merging any matching enrichment data.
     *
     * @param json input JSON string
     * @return enriched JSON string
     */
    public String enrich(String json) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }

        JsonNode keyNode = PipelinePlugin.fieldAt(input, keyField);
        if (keyNode == null || keyNode.isNull()) {
            return json;
        }
        String key = keyNode.asText();
        String enrichmentJson = inlineData.get(key);
        if (enrichmentJson == null) {
            return json;
        }

        JsonNode enrichment;
        try {
            enrichment = mapper.readTree(enrichmentJson);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid enrichment JSON for matching key", e);
        }

        return PipelinePlugin.shallowMerge(input, enrichment).toString();
    }

    /**
     * Returns the key field name used for lookups.
     */
    public String keyField() {
        return keyField;
    }

    /**
     * Returns an unmodifiable view of the inline enrichment data.
     */
    public Map<String, String> inlineData() {
        return Map.copyOf(inlineData);
    }
}
