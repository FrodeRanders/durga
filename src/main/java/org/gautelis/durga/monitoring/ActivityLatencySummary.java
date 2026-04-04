package org.gautelis.durga.monitoring;

/**
 * Query-time latency summary for one activity within one process definition.
 *
 * @param processId process definition identifier
 * @param activityId activity identifier
 * @param sampleCount number of completed samples included
 * @param averageDurationMs average observed duration in milliseconds
 * @param maxDurationMs maximum observed duration in milliseconds
 */
public record ActivityLatencySummary(
        String processId,
        String activityId,
        long sampleCount,
        long averageDurationMs,
        long maxDurationMs
) {
}
