package org.gautelis.durga.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gautelis.durga.ProcessEvent;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Outcome of comparing a validation-mode candidate output against the prior/production output for
 * the same shared input.
 * <p>
 * Because validation mode is handled per task, each candidate task is fed the production input for
 * that task in isolation; a divergence at an earlier task therefore does not cascade into the
 * comparison of later tasks. The report records both outputs and the located differences so a fix
 * that intentionally changes behaviour can be distinguished from a regression.
 */
public record ValidationResult(
        String processId,
        String taskId,
        String processInstanceId,
        String activityId,
        String priorVersion,
        String candidateVersion,
        Map<String, Object> inputPayload,
        Map<String, Object> priorOutput,
        Map<String, Object> candidateOutput,
        MatchStatus matchStatus,
        List<JsonComparison.Diff> diffs,
        String candidateErrorCode,
        String candidateErrorMessage,
        String timestamp
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum MatchStatus {
        /** Candidate output is identical to prior output after applying ignore paths. */
        EQUAL,
        /** Candidate output differs from prior output; see {@link #diffs()}. */
        DIFF,
        /** No prior/production output was found for this instance to compare against. */
        PRIOR_MISSING,
        /** The candidate implementation failed or requested a non-success error strategy. */
        CANDIDATE_ERROR
    }

    public ValidationResult {
        diffs = diffs != null ? List.copyOf(diffs) : List.of();
        timestamp = timestamp != null ? timestamp : Instant.now().toString();
    }

    public String key() {
        return ValidationCandidateOutput.key(processId, taskId, processInstanceId);
    }

    /**
     * Builds a comparison report from a candidate output and the paired prior/production output.
     *
     * @param candidate the shadow worker's candidate output (never {@code null})
     * @param priorOutput prior/production output payload for the same instance, or {@code null} if none was found
     * @param priorVersion process version that produced the prior output, or {@code null}
     * @param ignorePaths comparison ignore paths (see {@link JsonComparison})
     */
    /**
     * Builds a comparison report from a candidate output event from the validation events stream
     * and the paired prior/production output.
     *
     * @param candidate the validation process event (ACTIVITY_COMPLETED from a validation events topic; never null)
     * @param priorOutput prior/production output payload for the same instance, or null if none was found
     * @param priorVersion process version that produced the prior output, or null
     * @param ignorePaths comparison ignore paths (see {@link JsonComparison})
     */
    public static ValidationResult compare(ProcessEvent candidate,
                                           Map<String, Object> priorOutput,
                                           String priorVersion,
                                           Collection<String> ignorePaths) {
        MatchStatus status;
        List<JsonComparison.Diff> diffs = List.of();
        String errorCode = candidate.error() != null ? candidate.error().code() : null;
        String errorMessage = candidate.error() != null ? candidate.error().message() : null;

        boolean candidateFailed = candidate.status() == ProcessEvent.Status.FAILED || candidate.error() != null;
        if (candidateFailed) {
            status = MatchStatus.CANDIDATE_ERROR;
            if (errorMessage == null && candidate.status() == ProcessEvent.Status.FAILED) {
                errorMessage = "Validation candidate failed";
            }
            if (errorCode == null) {
                errorCode = "VALIDATION_CANDIDATE_FAILED";
            }
        } else if (priorOutput == null) {
            status = MatchStatus.PRIOR_MISSING;
        } else {
            JsonComparison.Report report =
                    JsonComparison.compare(priorOutput, candidate.payload(), ignorePaths);
            diffs = report.diffs();
            status = report.equal() ? MatchStatus.EQUAL : MatchStatus.DIFF;
        }

        return new ValidationResult(
                candidate.processId(),
                candidate.activityId(),
                candidate.processInstanceId(),
                candidate.activityId(),
                priorVersion,
                candidate.processVersion(),
                null, // input payload is not carried on ACTIVITY_COMPLETED events
                priorOutput,
                candidate.payload(),
                status,
                diffs,
                errorCode,
                errorMessage,
                Instant.now().toString()
        );
    }

    public static ValidationResult compare(ValidationCandidateOutput candidate,
                                           Map<String, Object> priorOutput,
                                           String priorVersion,
                                           Collection<String> ignorePaths) {
        MatchStatus status;
        List<JsonComparison.Diff> diffs = List.of();
        String errorCode = candidate.error() != null ? candidate.error().code() : null;
        String errorMessage = candidate.error() != null ? candidate.error().message() : null;

        boolean candidateFailed = candidate.errorStrategy() != null || candidate.error() != null;
        if (candidateFailed) {
            status = MatchStatus.CANDIDATE_ERROR;
            if (errorMessage == null) {
                errorMessage = candidate.sideEffectDescription();
            }
            if (errorCode == null && candidate.errorStrategy() != null) {
                errorCode = candidate.errorStrategy();
            }
        } else if (priorOutput == null) {
            status = MatchStatus.PRIOR_MISSING;
        } else {
            JsonComparison.Report report =
                    JsonComparison.compare(priorOutput, candidate.outputPayload(), ignorePaths);
            diffs = report.diffs();
            status = report.equal() ? MatchStatus.EQUAL : MatchStatus.DIFF;
        }

        return new ValidationResult(
                candidate.processId(),
                candidate.taskId(),
                candidate.processInstanceId(),
                candidate.activityId(),
                priorVersion,
                candidate.candidateVersion(),
                candidate.inputPayload(),
                priorOutput,
                candidate.outputPayload(),
                status,
                diffs,
                errorCode,
                errorMessage,
                Instant.now().toString()
        );
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ValidationResult", e);
        }
    }

    public static ValidationResult fromJson(String json) {
        try {
            return MAPPER.readValue(json, ValidationResult.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ValidationResult", e);
        }
    }
}
