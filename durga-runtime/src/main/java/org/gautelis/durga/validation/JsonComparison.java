package org.gautelis.durga.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Structural, order-sensitive JSON comparison used by validation mode to determine whether a
 * candidate task implementation produced the same output as the prior/production version for a
 * shared input.
 * <p>
 * Comparison is normalized by an optional set of <em>ignore paths</em>: dot-notation paths (with
 * {@code [n]} array indices) that are excluded from the comparison together with everything below
 * them. Ignore paths accept two wildcards:
 * <ul>
 *   <li>{@code *} matches any single object field segment;</li>
 *   <li>{@code [*]} matches any single array index segment.</li>
 * </ul>
 * For example {@code items[*].timestamp} ignores the {@code timestamp} field of every element of
 * the top-level {@code items} array. This lets callers suppress spurious differences such as
 * generated timestamps or identifiers while still flagging meaningful divergence.
 */
public final class JsonComparison {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_VALUE_LENGTH = 4000;

    private JsonComparison() {
    }

    /**
     * Classification of a single difference between prior and candidate output.
     */
    public enum DiffKind {
        /** Both sides hold a scalar of the same JSON type but different value. */
        VALUE_CHANGED,
        /** The two sides hold different JSON node types (e.g. object vs. array, string vs. number). */
        TYPE_CHANGED,
        /** Present only in the candidate output. */
        ADDED,
        /** Present only in the prior output. */
        REMOVED
    }

    /**
     * A single located difference. Values are rendered as compact JSON and truncated for transport.
     *
     * @param path normalized location, e.g. {@code order.lines[0].price}
     * @param kind classification of the difference
     * @param priorValue prior/production value, or {@code null} when absent
     * @param candidateValue candidate value, or {@code null} when absent
     */
    public record Diff(String path, DiffKind kind, String priorValue, String candidateValue) {
    }

    /**
     * Result of a comparison.
     *
     * @param equal {@code true} when no differences remain after applying ignore paths
     * @param diffs ordered list of differences (empty when {@code equal})
     */
    public record Report(boolean equal, List<Diff> diffs) {
    }

    public static Report compare(String priorJson, String candidateJson, Collection<String> ignorePaths) {
        return compare(readTree(priorJson), readTree(candidateJson), ignorePaths);
    }

    public static Report compare(Map<String, Object> prior, Map<String, Object> candidate,
                                 Collection<String> ignorePaths) {
        JsonNode priorNode = MAPPER.valueToTree(prior);
        JsonNode candidateNode = MAPPER.valueToTree(candidate);
        return compare(priorNode, candidateNode, ignorePaths);
    }

    public static Report compare(JsonNode prior, JsonNode candidate, Collection<String> ignorePaths) {
        List<List<String>> compiled = compileIgnorePaths(ignorePaths);
        List<Diff> diffs = new ArrayList<>();
        diff(new ArrayList<>(), normalize(prior), normalize(candidate), compiled, diffs);
        return new Report(diffs.isEmpty(), List.copyOf(diffs));
    }

    private static JsonNode normalize(JsonNode node) {
        return node == null ? MAPPER.nullNode() : node;
    }

    private static JsonNode readTree(String json) {
        if (json == null || json.isBlank()) {
            return MAPPER.nullNode();
        }
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to parse JSON for comparison", e);
        }
    }

    private static void diff(List<String> path, JsonNode prior, JsonNode candidate,
                             List<List<String>> ignorePaths, List<Diff> diffs) {
        if (isIgnored(path, ignorePaths)) {
            return;
        }
        boolean priorMissing = prior == null || prior.isMissingNode();
        boolean candidateMissing = candidate == null || candidate.isMissingNode();
        if (priorMissing && candidateMissing) {
            return;
        }
        if (priorMissing) {
            diffs.add(new Diff(render(path), DiffKind.ADDED, null, value(candidate)));
            return;
        }
        if (candidateMissing) {
            diffs.add(new Diff(render(path), DiffKind.REMOVED, value(prior), null));
            return;
        }
        if (!sameNodeShape(prior, candidate)) {
            diffs.add(new Diff(render(path), DiffKind.TYPE_CHANGED, value(prior), value(candidate)));
            return;
        }
        if (prior.isObject()) {
            Set<String> fields = new LinkedHashSet<>();
            prior.fieldNames().forEachRemaining(fields::add);
            candidate.fieldNames().forEachRemaining(fields::add);
            for (String field : fields) {
                path.add(field);
                diff(path, prior.get(field), candidate.get(field), ignorePaths, diffs);
                path.remove(path.size() - 1);
            }
            return;
        }
        if (prior.isArray()) {
            int max = Math.max(prior.size(), candidate.size());
            for (int i = 0; i < max; i++) {
                path.add("[" + i + "]");
                diff(path, i < prior.size() ? prior.get(i) : null,
                        i < candidate.size() ? candidate.get(i) : null, ignorePaths, diffs);
                path.remove(path.size() - 1);
            }
            return;
        }
        if (!prior.equals(candidate)) {
            diffs.add(new Diff(render(path), DiffKind.VALUE_CHANGED, value(prior), value(candidate)));
        }
    }

    private static boolean sameNodeShape(JsonNode a, JsonNode b) {
        if (a.isObject()) {
            return b.isObject();
        }
        if (a.isArray()) {
            return b.isArray();
        }
        if (a.isNull()) {
            return b.isNull();
        }
        if (a.isNumber()) {
            return b.isNumber();
        }
        if (a.isBoolean()) {
            return b.isBoolean();
        }
        if (a.isTextual()) {
            return b.isTextual();
        }
        return a.getNodeType() == b.getNodeType();
    }

    private static String value(JsonNode node) {
        if (node == null || node.isMissingNode()) {
            return null;
        }
        String rendered = node.toString();
        if (rendered.length() > MAX_VALUE_LENGTH) {
            return rendered.substring(0, MAX_VALUE_LENGTH) + "\u2026";
        }
        return rendered;
    }

    private static String render(List<String> path) {
        StringBuilder sb = new StringBuilder();
        for (String segment : path) {
            if (segment.startsWith("[")) {
                sb.append(segment);
            } else {
                if (sb.length() > 0) {
                    sb.append('.');
                }
                sb.append(segment);
            }
        }
        return sb.toString();
    }

    private static List<List<String>> compileIgnorePaths(Collection<String> ignorePaths) {
        List<List<String>> compiled = new ArrayList<>();
        if (ignorePaths == null) {
            return compiled;
        }
        Set<String> unique = new TreeSet<>();
        for (String raw : ignorePaths) {
            if (raw != null && !raw.isBlank()) {
                unique.add(raw.trim());
            }
        }
        for (String pattern : unique) {
            List<String> tokens = tokenize(pattern);
            if (!tokens.isEmpty()) {
                compiled.add(tokens);
            }
        }
        return compiled;
    }

    private static boolean isIgnored(List<String> path, List<List<String>> ignorePaths) {
        for (List<String> pattern : ignorePaths) {
            if (pattern.size() > path.size()) {
                continue;
            }
            boolean matches = true;
            for (int i = 0; i < pattern.size(); i++) {
                if (!tokenMatches(pattern.get(i), path.get(i))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return true;
            }
        }
        return false;
    }

    private static boolean tokenMatches(String pattern, String actual) {
        if (pattern.equals(actual)) {
            return true;
        }
        boolean actualIsIndex = actual.startsWith("[");
        if (pattern.equals("[*]")) {
            return actualIsIndex;
        }
        if (pattern.equals("*")) {
            return !actualIsIndex;
        }
        return false;
    }

    private static List<String> tokenize(String pattern) {
        List<String> tokens = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            switch (ch) {
                case '.' -> flush(buffer, tokens);
                case '[' -> {
                    flush(buffer, tokens);
                    buffer.append('[');
                }
                case ']' -> {
                    buffer.append(']');
                    tokens.add(buffer.toString());
                    buffer.setLength(0);
                }
                default -> buffer.append(ch);
            }
        }
        flush(buffer, tokens);
        return tokens;
    }

    private static void flush(StringBuilder buffer, List<String> tokens) {
        if (buffer.length() > 0) {
            tokens.add(buffer.toString());
            buffer.setLength(0);
        }
    }

    static List<String> parsePathForTest(String pattern) {
        return tokenize(pattern);
    }
}
