package org.gautelis.durga.monitoring;

import org.gautelis.durga.validation.ValidationResult;

/**
 * Aggregated validation outcome counts for one process/task under validation, derived from the
 * per-instance {@link ValidationResult} records.
 */
public record ValidationSummary(
        String processId,
        String taskId,
        long equal,
        long diff,
        long priorMissing,
        long candidateError,
        long total
) {
    public static ValidationSummary empty(String processId, String taskId) {
        return new ValidationSummary(processId, taskId, 0, 0, 0, 0, 0);
    }

    /**
     * Returns a copy of this summary with the supplied result counted.
     */
    public ValidationSummary add(ValidationResult result) {
        long e = equal;
        long d = diff;
        long pm = priorMissing;
        long ce = candidateError;
        switch (result.matchStatus()) {
            case EQUAL -> e++;
            case DIFF -> d++;
            case PRIOR_MISSING -> pm++;
            case CANDIDATE_ERROR -> ce++;
        }
        return new ValidationSummary(processId, taskId, e, d, pm, ce, total + 1);
    }
}
