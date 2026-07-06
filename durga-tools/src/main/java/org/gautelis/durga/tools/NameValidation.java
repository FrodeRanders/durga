package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.StartEvent;
import org.gautelis.durga.NameNormalizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates that a BPMN model's process id and element names produce safe, unambiguous
 * identifiers once normalized (see {@link NameNormalizer}). Normalized names become both
 * Kafka topic segments and generated Java identifiers, so they must be non-empty, must not
 * start with a digit, must not be Java reserved words, and must not collide with one another.
 */
final class NameValidation {

    enum Severity { WARNING, ERROR }

    record Issue(Severity severity, String message) {
    }

    private NameValidation() {
    }

    /**
     * @param model            the parsed BPMN model
     * @param rawProcessSource the raw process id source (the {@code --process-id} override if
     *                         given, otherwise the model's {@code <process id>})
     * @return issues found, in reporting order (warnings and errors)
     */
    static List<Issue> validate(BpmnModelInstance model, String rawProcessSource) {
        List<Issue> issues = new ArrayList<>();

        checkIdentifier(issues, "process id", rawProcessSource);

        // Every non-start/end flow node becomes a generated class and/or topic segment.
        Map<String, Set<String>> slugToRaw = new LinkedHashMap<>();
        for (FlowNode node : model.getModelElementsByType(FlowNode.class)) {
            if (node instanceof StartEvent || node instanceof EndEvent) {
                continue;
            }
            String raw = BpmnModelCollector.nameOrId(node.getName(), node.getId());
            checkIdentifier(issues, "element '" + raw + "'", raw);
            String slug = NameNormalizer.slug(raw);
            if (!slug.isEmpty()) {
                slugToRaw.computeIfAbsent(slug, k -> new LinkedHashSet<>()).add(raw);
            }
        }

        for (var entry : slugToRaw.entrySet()) {
            if (entry.getValue().size() > 1) {
                issues.add(new Issue(Severity.ERROR,
                        "name collision: " + entry.getValue()
                                + " all normalize to '" + entry.getKey()
                                + "', which would share generated classes and topics"));
            }
        }

        return issues;
    }

    static boolean hasErrors(List<Issue> issues) {
        return issues.stream().anyMatch(i -> i.severity() == Severity.ERROR);
    }

    private static void checkIdentifier(List<Issue> issues, String what, String raw) {
        String slug = NameNormalizer.slug(raw);
        if (slug.isEmpty()) {
            issues.add(new Issue(Severity.ERROR,
                    what + " '" + raw + "' contains no usable ASCII letters or digits; "
                            + "cannot derive a valid identifier"));
            return;
        }
        if (NameNormalizer.startsWithDigit(slug)) {
            issues.add(new Issue(Severity.ERROR,
                    what + " normalizes to '" + slug
                            + "', which starts with a digit and is not a valid Java identifier"));
        }
        if (NameNormalizer.isReservedJavaWord(slug)) {
            issues.add(new Issue(Severity.ERROR,
                    what + " normalizes to Java reserved word '" + slug + "'"));
        }
        if (containsNonAscii(raw)) {
            issues.add(new Issue(Severity.WARNING,
                    what + " was transliterated to '" + slug + "'"));
        }
    }

    private static boolean containsNonAscii(String value) {
        if (value == null) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0x7F) {
                return true;
            }
        }
        return false;
    }
}
