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
            int eq = config.indexOf('=');
            if (eq > 0) {
                String key = config.substring(0, eq).trim();
                String val = config.substring(eq + 1).trim();
                switch (key) {
                    case "keyField" -> keyField = val;
                    case "inline" -> {
                        if (val.startsWith("{") && val.endsWith("}")) {
                            val = val.substring(1, val.length() - 1);
                            for (String pair : val.split("\\s*,\\s*")) {
                                int colon = pair.indexOf(':');
                                if (colon > 0) {
                                    inline.put(pair.substring(0, colon).trim(),
                                            pair.substring(colon + 1).trim());
                                }
                            }
                        }
                    }
                }
            }
        }
        KvEnricher enricher = new KvEnricher(keyField, inline);
        return enricher.enrich(payload);
    }

    private final String keyField;
    private final Map<String, String> inlineData;

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
            throw new IllegalArgumentException("Invalid enrichment JSON for key '" + key + "'", e);
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
