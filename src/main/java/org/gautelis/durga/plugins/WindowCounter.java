package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Counts messages within a time window and emits a summary record when the
 * window closes.
 *
 * <p>If {@code groupBy} is set, counts are grouped by the value of that field.
 */
public final class WindowCounter implements Plugin {

    private static final int MAX_GROUPS = 10_000;
    private static final int MAX_GROUP_VALUE_LENGTH = 256;

    private long windowMs;
    private String groupBy;
    private ObjectMapper mapper;
    private long currentWindowStart = -1;
    private long totalCount;
    private final Map<String, Long> groupCounts = new LinkedHashMap<>();
    private boolean initialized;

    @Override
    public synchronized String execute(String payload, String config) throws Exception {
        if (!initialized) {
            long windowSecs = 60;
            String group = null;
            if (config != null && !config.isBlank()) {
                String[] parts = config.split("\\s+");
                for (String part : parts) {
                    int eq = part.indexOf('=');
                    if (eq > 0) {
                        String key = part.substring(0, eq).trim();
                        String val = part.substring(eq + 1).trim();
                        switch (key) {
                            case "window" -> windowSecs = Long.parseLong(val);
                            case "groupBy" -> group = val;
                        }
                    }
                }
            }
            if (windowSecs <= 0) {
                throw new IllegalArgumentException("Window must be greater than zero seconds");
            }
            this.windowMs = windowSecs * 1000L;
            this.groupBy = group;
            this.mapper = new ObjectMapper();
            this.initialized = true;
        }
        return accept(payload);
    }

    /** No-arg constructor for plugin interface. */
    public WindowCounter() {
    }

    public WindowCounter(long windowSeconds, String groupBy) {
        this.windowMs = windowSeconds * 1000L;
        this.groupBy = groupBy;
        this.mapper = new ObjectMapper();
        this.initialized = true;
    }

    synchronized String accept(String json) {
        long now = System.currentTimeMillis();
        long windowBucket = (now / windowMs) * windowMs;

        String summary = null;
        if (currentWindowStart >= 0 && windowBucket != currentWindowStart) {
            summary = buildSummary();
            totalCount = 0;
            groupCounts.clear();
        }

        currentWindowStart = windowBucket;
        totalCount++;

        if (groupBy != null && !groupBy.isBlank()) {
            try {
                JsonNode node = mapper.readTree(json);
                JsonNode groupNode = PipelinePlugin.fieldAt(node, groupBy);
                String group = groupNode != null && !groupNode.isNull()
                        ? groupNode.asText()
                        : "_null_";
                mergeGroup(group);
            } catch (JsonProcessingException e) {
                mergeGroup("_parse_error_");
            }
        }

        return summary;
    }

    private void mergeGroup(String group) {
        if (group.length() > MAX_GROUP_VALUE_LENGTH) {
            group = group.substring(0, MAX_GROUP_VALUE_LENGTH);
        }
        if (!groupCounts.containsKey(group) && groupCounts.size() >= MAX_GROUPS) {
            group = "_overflow_";
        }
        groupCounts.merge(group, 1L, Long::sum);
    }

    public synchronized String flush() {
        String summary = buildSummary();
        totalCount = 0;
        groupCounts.clear();
        return summary;
    }

    private String buildSummary() {
        ObjectNode summary = mapper.createObjectNode();
        summary.put("windowStart", currentWindowStart);
        summary.put("windowEnd", currentWindowStart + windowMs);
        summary.put("totalCount", totalCount);
        if (groupBy != null && !groupBy.isBlank() && !groupCounts.isEmpty()) {
            ObjectNode groups = mapper.createObjectNode();
            groupCounts.forEach(groups::put);
            summary.set("groupCounts", groups);
        }
        return summary.toString();
    }

    public long windowSeconds() {
        return windowMs / 1000L;
    }

    public String groupBy() {
        return groupBy;
    }
}
