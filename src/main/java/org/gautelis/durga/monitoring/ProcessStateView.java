package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Latest materialized monitoring view for a single process instance.
 * <p>
 * This projection is derived from the canonical lifecycle stream and keeps only the current view
 * needed for query-time inspection, counts, latency summaries, and stuck-instance detection.
 */
public record ProcessStateView(
        String processInstanceId,
        String processId,
        String processVersion,
        String currentActivityId,
        String lifecycleState,
        String startedAt,
        String lastUpdatedAt,
        String completedAt,
        String correlationId,
        String businessKey,
        int retryCount,
        Map<String, String> activityEnteredAt,
        Map<String, Long> activityDurationsMs,
        String lastErrorCode,
        String lastErrorMessage
) {
    public static final String UNKNOWN_STATE = "unknown";

    /**
     * Creates an empty projection seed for Kafka Streams aggregation.
     *
     * @return empty instance view
     */
    public static ProcessStateView empty() {
        return new ProcessStateView(
                null,
                null,
                null,
                null,
                UNKNOWN_STATE,
                null,
                null,
                null,
                null,
                null,
                0,
                Map.of(),
                Map.of(),
                null,
                null
        );
    }

    /**
     * Applies one lifecycle event to the current projection state.
     *
     * @param event lifecycle event from the canonical stream
     * @return updated instance view
     */
    public ProcessStateView apply(ProcessEvent event) {
        Instant eventTimestamp = parseTimestamp(event.timestamp());
        Map<String, String> enteredAt = new LinkedHashMap<>(activityEnteredAt);
        Map<String, Long> durations = new LinkedHashMap<>(activityDurationsMs);

        // The projection keeps only "latest useful truth" per instance, but it still preserves
        // enough timing data to answer latency and stuck-instance queries later.
        String newStartedAt = startedAt;
        String newCompletedAt = completedAt;
        int newRetryCount = retryCount;
        String newCurrentActivityId = event.activityId();
        String newLifecycleState = lifecycleStateFor(event);
        String newErrorCode = event.error() != null ? event.error().code() : null;
        String newErrorMessage = event.error() != null ? event.error().message() : null;

        switch (event.eventType()) {
            case PROCESS_STARTED -> newStartedAt = newStartedAt != null ? newStartedAt : eventTimestamp.toString();
            case ACTIVITY_ENTERED -> {
                if (event.activityId() != null && !event.activityId().isBlank()) {
                    enteredAt.put(event.activityId(), eventTimestamp.toString());
                }
            }
            case ACTIVITY_COMPLETED, ACTIVITY_ESCALATED, ACTIVITY_CANCELLED, GATEWAY_TAKEN -> recordDuration(event.activityId(), eventTimestamp, enteredAt, durations);
            case PROCESS_COMPLETED -> {
                recordDuration(event.activityId(), eventTimestamp, enteredAt, durations);
                newCompletedAt = eventTimestamp.toString();
                newCurrentActivityId = "completed";
            }
            case PROCESS_FAILED -> {
                newRetryCount = retryCount + 1;
                recordDuration(event.activityId(), eventTimestamp, enteredAt, durations);
            }
        }

        return new ProcessStateView(
                firstNonBlank(event.processInstanceId(), processInstanceId),
                firstNonBlank(event.processId(), processId),
                firstNonBlank(event.processVersion(), processVersion),
                firstNonBlank(newCurrentActivityId, currentActivityId),
                newLifecycleState,
                newStartedAt,
                eventTimestamp.toString(),
                newCompletedAt,
                firstNonBlank(event.correlationId(), correlationId),
                firstNonBlank(event.businessKey(), businessKey),
                newRetryCount,
                Map.copyOf(enteredAt),
                Map.copyOf(durations),
                newErrorCode,
                newErrorMessage
        );
    }

    /**
     * Returns the key shape used by the aggregate count store.
     *
     * @return {@code processId:state} style key
     */
    public String currentStateKey() {
        String pid = processId != null && !processId.isBlank() ? processId : "unknown";
        String state = currentActivityId != null && !currentActivityId.isBlank()
                ? currentActivityId
                : lifecycleState != null && !lifecycleState.isBlank() ? lifecycleState : UNKNOWN_STATE;
        return pid + ":" + state;
    }

    private static void recordDuration(
            String activityId,
            Instant eventTimestamp,
            Map<String, String> enteredAt,
            Map<String, Long> durations
    ) {
        if (activityId == null || activityId.isBlank()) {
            return;
        }
        String entered = enteredAt.get(activityId);
        if (entered == null) {
            return;
        }
        // Durations are overwritten per activity id, so the view tracks the latest observed
        // duration for that activity within the instance rather than a full history.
        long durationMs = Math.max(0L, Duration.between(Instant.parse(entered), eventTimestamp).toMillis());
        durations.put(activityId, durationMs);
    }

    private static String lifecycleStateFor(ProcessEvent event) {
        return switch (event.eventType()) {
            case PROCESS_STARTED, ACTIVITY_ENTERED, ACTIVITY_COMPLETED, ACTIVITY_ESCALATED, GATEWAY_TAKEN -> "active";
            case ACTIVITY_CANCELLED -> "cancelled";
            case PROCESS_COMPLETED -> "completed";
            case PROCESS_FAILED -> "failed";
        };
    }

    private static String firstNonBlank(String preferred, String fallback) {
        return preferred != null && !preferred.isBlank() ? preferred : fallback;
    }

    private static Instant parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return Instant.now();
        }
        return Instant.parse(timestamp);
    }
}
