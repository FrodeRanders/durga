package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts named capture groups from a field using a regular expression and
 * stores the results in the payload.
 *
 * <p>Config syntax:
 * <pre>
 * "source=log_line;pattern=(?&lt;ip&gt;\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(?&lt;method&gt;\\w+);target=parsed"
 * </pre>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code source} — dot-notation path to the source field (required)</li>
 *   <li>{@code pattern} — Java regex with named groups (required)</li>
 *   <li>{@code target} — dot-notation path to store extracted groups (optional, default: top-level)</li>
 *   <li>{@code all} — if "true", extract all matches (optional, default matches first only)</li>
 * </ul>
 */
public final class RegexExtract implements Plugin {

    @Override
    public byte[] execute(byte[] payload, String config) throws Exception {
        String payloadStr = Plugin.toString(payload);
        String source = null;
        String pattern = null;
        String target = null;
        boolean all = false;
        if (config != null && !config.isBlank()) {
            String[] parts = config.split(";");
            for (String part : parts) {
                part = part.trim();
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "source" -> source = val;
                        case "pattern" -> pattern = val;
                        case "target" -> target = val;
                        case "all" -> all = "true".equalsIgnoreCase(val);
                    }
                }
            }
        }
        return Plugin.toBytes(extract(payloadStr, source, pattern, target, all));
    }

    private RegexExtract() {
    }

    public static String extract(String json, String sourceField, String regex, String targetPath, boolean findAll) {
        if (sourceField == null || regex == null) {
            return json;
        }
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

        JsonNode sourceNode = PipelinePlugin.fieldAt(input, sourceField);
        if (sourceNode == null || !sourceNode.isTextual()) {
            return json;
        }

        String text = sourceNode.asText();
        Pattern compiled = Pattern.compile(regex, findAll ? 0 : 0);

        ObjectNode output = input.deepCopy();
        Matcher matcher = compiled.matcher(text);

        if (findAll) {
            boolean found = false;
            while (matcher.find()) {
                found = true;
                applyGroups(mapper, output, matcher, targetPath);
            }
            if (!found) {
                return json;
            }
        } else {
            if (!matcher.find()) {
                return json;
            }
            applyGroups(mapper, output, matcher, targetPath);
        }

        return output.toString();
    }

    private static void applyGroups(ObjectMapper mapper, ObjectNode root, Matcher matcher, String targetPath) {
        ObjectNode target;
        if (targetPath != null && !targetPath.isBlank()) {
            JsonNode existing = PipelinePlugin.fieldAt(root, targetPath);
            if (existing != null && existing.isObject()) {
                target = (ObjectNode) existing;
            } else {
                target = mapper.createObjectNode();
                PipelinePlugin.setFieldAt(root, targetPath, target);
            }
        } else {
            target = root;
        }

        Matcher groupMatcher = Pattern.compile("\\(\\?<(\\w+)>").matcher(matcher.pattern().pattern());
        java.util.Set<String> groupNames = new java.util.LinkedHashSet<>();
        while (groupMatcher.find()) {
            groupNames.add(groupMatcher.group(1));
        }

        for (String name : groupNames) {
            try {
                String value = matcher.group(name);
                if (value != null) {
                    target.put(name, value);
                }
            } catch (IllegalArgumentException ignored) {
            }
        }

        if (groupNames.isEmpty()) {
            int groupCount = matcher.groupCount();
            for (int i = 1; i <= groupCount; i++) {
                String value = matcher.group(i);
                if (value != null) {
                    target.put("group" + i, value);
                }
            }
        }
    }
}
