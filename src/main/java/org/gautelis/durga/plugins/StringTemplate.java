package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders a template string with payload field substitution.
 *
 * <p>Config syntax:
 * <pre>
 * "template=Hello ${name}, your order ${id} totals ${amount}"
 * </pre>
 *
 * <p>Fields support dot-notation for nested access (e.g. {@code ${customer.name}}).
 * Missing fields are replaced with an empty string.
 */
public final class StringTemplate implements Plugin {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    @Override
    public String execute(String payload, String config) throws Exception {
        String template = null;
        if (config != null && !config.isBlank()) {
            String[] parts = config.split("\\s+", 2);
            for (String part : parts) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    if ("template".equals(key)) {
                        template = val;
                    }
                }
            }
        }
        if (template == null) {
            return payload;
        }
        return render(payload, template);
    }

    private StringTemplate() {
    }

    public static String render(String json, String template) {
        if (template == null || template.isBlank()) {
            return json;
        }
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node;
        try {
            node = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }

        Matcher matcher = TOKEN_PATTERN.matcher(template);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String fieldPath = matcher.group(1).trim();
            JsonNode fieldNode = PipelinePlugin.fieldAt(node, fieldPath);
            String replacement = "";
            if (fieldNode != null && !fieldNode.isNull()) {
                if (fieldNode.isTextual()) {
                    replacement = fieldNode.asText();
                } else {
                    replacement = fieldNode.toString();
                }
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
