package org.gautelis.durga.validation;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ValidationResultTest {

    private static ValidationCandidateOutput candidate(Map<String, Object> input,
                                                       Map<String, Object> output,
                                                       String errorStrategy,
                                                       ProcessEvent.ErrorInfo error) {
        return new ValidationCandidateOutput(
                "p", "t", "inst-1", "t", "tok", "corr", "biz", "v2",
                input, output, "PAYLOAD", null, "idem", errorStrategy, error, "2026-01-01T00:00:00Z");
    }

    @Test
    public void shouldMarkEqualWhenOutputsMatch() {
        System.out.println("TC: matching prior and candidate output yields EQUAL with no diffs");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), Map.of("out", 2), null, null),
                Map.of("out", 2), "v1", List.of());
        assertEquals(ValidationResult.MatchStatus.EQUAL, result.matchStatus());
        assertTrue(result.diffs().isEmpty());
        assertEquals("v1", result.priorVersion());
        assertEquals("v2", result.candidateVersion());
    }

    @Test
    public void shouldMarkDiffWithLocatedDifferences() {
        System.out.println("TC: differing outputs yield DIFF carrying the located differences");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), Map.of("out", 3), null, null),
                Map.of("out", 2), "v1", List.of());
        assertEquals(ValidationResult.MatchStatus.DIFF, result.matchStatus());
        assertEquals(1, result.diffs().size());
        assertEquals("out", result.diffs().get(0).path());
    }

    @Test
    public void shouldRespectIgnorePaths() {
        System.out.println("TC: ignore paths suppress volatile differences so outputs compare EQUAL");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), Map.of("out", 2, "processedAt", "later"), null, null),
                Map.of("out", 2, "processedAt", "earlier"), "v1", List.of("processedAt"));
        assertEquals(ValidationResult.MatchStatus.EQUAL, result.matchStatus());
    }

    @Test
    public void shouldMarkPriorMissingWhenNoProductionOutput() {
        System.out.println("TC: absent prior output yields PRIOR_MISSING");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), Map.of("out", 2), null, null),
                null, null, List.of());
        assertEquals(ValidationResult.MatchStatus.PRIOR_MISSING, result.matchStatus());
    }

    @Test
    public void shouldMarkCandidateErrorFromErrorStrategy() {
        System.out.println("TC: a candidate error strategy yields CANDIDATE_ERROR");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), null, "FAIL",
                        new ProcessEvent.ErrorInfo("boom", "PLUGIN_FAILED")),
                Map.of("out", 2), "v1", List.of());
        assertEquals(ValidationResult.MatchStatus.CANDIDATE_ERROR, result.matchStatus());
        assertEquals("PLUGIN_FAILED", result.candidateErrorCode());
        assertEquals("boom", result.candidateErrorMessage());
    }

    @Test
    public void shouldRoundTripThroughJson() {
        System.out.println("TC: ValidationResult survives JSON round-trip");
        ValidationResult result = ValidationResult.compare(
                candidate(Map.of("in", 1), Map.of("out", 3), null, null),
                Map.of("out", 2), "v1", List.of());
        ValidationResult parsed = ValidationResult.fromJson(result.toJson());
        assertEquals(result.matchStatus(), parsed.matchStatus());
        assertEquals(result.diffs().size(), parsed.diffs().size());
        assertEquals(result.key(), parsed.key());
    }
}
