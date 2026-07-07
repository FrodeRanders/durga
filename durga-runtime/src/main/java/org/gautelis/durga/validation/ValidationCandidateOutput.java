package org.gautelis.durga.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.gautelis.durga.ProcessEvent;

import java.time.Instant;
import java.util.Map;

/**
 * Output of a task run in validation mode by a shadow worker.
 * <p>
 * This is the retained DTO for the retired first validation-mode design, where a shadow worker
 * emitted an explicit candidate-output record instead of a normal lifecycle event. It carries both
 * the shared {@code inputPayload} and the candidate {@code outputPayload} so a comparator can pair
 * it against the prior/production output for the same
 * {@code (processId, activityId, processInstanceId)} and produce a {@link ValidationResult}.
 * <p>
 * Records are keyed on the Kafka topic by {@link #key()}.
 */
public record ValidationCandidateOutput(
        String processId,
        String taskId,
        String processInstanceId,
        String activityId,
        String tokenId,
        String correlationId,
        String businessKey,
        String candidateVersion,
        Map<String, Object> inputPayload,
        Map<String, Object> outputPayload,
        String disposition,
        String sideEffectDescription,
        String idempotencyKey,
        String errorStrategy,
        ProcessEvent.ErrorInfo error,
        String timestamp
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ValidationCandidateOutput {
        timestamp = timestamp != null ? timestamp : Instant.now().toString();
    }

    /**
     * Stable partition/lookup key: {@code processId:taskId:processInstanceId}. The comparator joins
     * against the prior output using the same key so live-concurrent and historic replay share one
     * matching mechanism.
     */
    public String key() {
        return key(processId, taskId, processInstanceId);
    }

    /**
     * Builds the join key shared with the prior-output side of the comparison. {@code activityId}
     * equals {@code taskId} for a validated task, so the prior {@code ACTIVITY_COMPLETED} event for
     * the same instance resolves to this same key.
     */
    public static String key(String processId, String taskId, String processInstanceId) {
        return processId + ":" + taskId + ":" + processInstanceId;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ValidationCandidateOutput", e);
        }
    }

    public static ValidationCandidateOutput fromJson(String json) {
        try {
            return MAPPER.readValue(json, ValidationCandidateOutput.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ValidationCandidateOutput", e);
        }
    }
}
