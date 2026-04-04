package org.gautelis.durga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;

/**
 * Canonical lifecycle event emitted by generated runtimes and consumed by monitoring projections.
 * <p>
 * Events are keyed by {@code processInstanceId} and carry enough context to reconstruct current
 * process state, failures, and timing information from the shared {@code process-events} topic.
 */
public record ProcessEvent(
        String processInstanceId,
        String processId,
        String activityId,
        String tokenId,
        String correlationId,
        Map<String, Object> payload,
        Status status,
        ErrorInfo error,
        EventType eventType,
        String processVersion,
        String businessKey,
        String timestamp
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Coarse-grained execution status attached to a lifecycle event.
     */
    public enum Status {
        STARTED,
        COMPLETED,
        FAILED,
        ESCALATED,
        CANCELLED
    }

    /**
     * More specific lifecycle classification used by monitoring and generated control flow.
     */
    public enum EventType {
        PROCESS_STARTED,
        ACTIVITY_ENTERED,
        ACTIVITY_COMPLETED,
        ACTIVITY_ESCALATED,
        ACTIVITY_CANCELLED,
        GATEWAY_TAKEN,
        PROCESS_COMPLETED,
        PROCESS_FAILED
    }

    /**
     * Optional structured error payload for failures and escalations.
     *
     * @param message human-readable description
     * @param code domain-specific error or escalation code
     */
    public record ErrorInfo(String message, String code) {
    }

    /**
     * Creates a lifecycle event using the legacy compact signature.
     * <p>
     * The event type is inferred from {@code status} and the timestamp defaults to the current
     * instant.
     */
    public ProcessEvent(
            String processInstanceId,
            String processId,
            String activityId,
            String tokenId,
            String correlationId,
            Map<String, Object> payload,
            Status status,
            ErrorInfo error
    ) {
        this(
                processInstanceId,
                processId,
                activityId,
                tokenId,
                correlationId,
                payload,
                status,
                error,
                inferEventType(status),
                null,
                null,
                Instant.now().toString()
        );
    }

    public ProcessEvent {
        eventType = eventType != null ? eventType : inferEventType(status);
        timestamp = timestamp != null ? timestamp : Instant.now().toString();
    }

    private static EventType inferEventType(Status status) {
        if (status == null) {
            return EventType.ACTIVITY_ENTERED;
        }
        return switch (status) {
            case STARTED -> EventType.PROCESS_STARTED;
            case COMPLETED -> EventType.ACTIVITY_COMPLETED;
            case FAILED -> EventType.PROCESS_FAILED;
            case ESCALATED -> EventType.ACTIVITY_ESCALATED;
            case CANCELLED -> EventType.ACTIVITY_CANCELLED;
        };
    }

    /**
     * Serializes this event as JSON for Kafka transport.
     *
     * @return JSON representation of this event
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ProcessEvent", e);
        }
    }

    /**
     * Parses a previously serialized lifecycle event.
     *
     * @param json JSON produced by {@link #toJson()}
     * @return parsed event instance
     */
    public static ProcessEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, ProcessEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ProcessEvent", e);
        }
    }
}
