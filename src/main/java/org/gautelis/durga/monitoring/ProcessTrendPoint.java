package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Coarse history bucket for lifecycle trends such as starts, completions, and failures.
 *
 * @param processId process definition identifier
 * @param bucketStartedAt minute bucket start in ISO-8601 format
 * @param metric trend metric name
 * @param count number of matching lifecycle events in the bucket
 */
public record ProcessTrendPoint(
        String processId,
        String bucketStartedAt,
        String metric,
        long count
) {
    static boolean supportedEventType(ProcessEvent.EventType eventType) {
        return eventType == ProcessEvent.EventType.PROCESS_STARTED
                || eventType == ProcessEvent.EventType.PROCESS_COMPLETED
                || eventType == ProcessEvent.EventType.PROCESS_FAILED;
    }

    static String keyFor(ProcessEvent event) {
        String processId = event.processId() != null && !event.processId().isBlank() ? event.processId() : "unknown";
        String bucketStartedAt = bucketFor(event.timestamp());
        String metric = switch (event.eventType()) {
            case PROCESS_STARTED -> "started";
            case PROCESS_COMPLETED -> "completed";
            case PROCESS_FAILED -> "failed";
            default -> "other";
        };
        return processId + ":" + bucketStartedAt + ":" + metric;
    }

    /**
     * Parses a materialized trend-store key of the form {@code processId:bucketStartedAt:metric}.
     *
     * @param trendKey store key
     * @param count count value
     * @return structured trend record
     */
    public static ProcessTrendPoint fromKey(String trendKey, long count) {
        int firstSeparator = trendKey.indexOf(':');
        int lastSeparator = trendKey.lastIndexOf(':');
        String processId = firstSeparator > 0 ? trendKey.substring(0, firstSeparator) : "unknown";
        String bucketStartedAt = firstSeparator >= 0 && lastSeparator > firstSeparator
                ? trendKey.substring(firstSeparator + 1, lastSeparator)
                : Instant.EPOCH.toString();
        String metric = lastSeparator > firstSeparator ? trendKey.substring(lastSeparator + 1) : "unknown";
        return new ProcessTrendPoint(processId, bucketStartedAt, metric, count);
    }

    private static String bucketFor(String timestamp) {
        Instant instant = (timestamp == null || timestamp.isBlank()) ? Instant.now() : Instant.parse(timestamp);
        return instant.truncatedTo(ChronoUnit.MINUTES).toString();
    }
}
