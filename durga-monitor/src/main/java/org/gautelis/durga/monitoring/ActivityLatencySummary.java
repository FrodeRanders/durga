package org.gautelis.durga.monitoring;

/**
 * Query-time latency summary for one activity within one process definition.
 *
 * @param processId process definition identifier
 * @param activityId activity identifier
 * @param sampleCount number of completed samples included
 * @param averageDurationMs average observed duration in milliseconds
 * @param maxDurationMs maximum observed duration in milliseconds
 * @param p50DurationMs 50th percentile duration
 * @param p95DurationMs 95th percentile duration
 * @param p99DurationMs 99th percentile duration
 * @param slaViolationCount number of samples exceeding the configured SLA threshold
 */
public record ActivityLatencySummary(
        String processId,
        String activityId,
        long sampleCount,
        long averageDurationMs,
        long maxDurationMs,
        long p50DurationMs,
        long p95DurationMs,
        long p99DurationMs,
        long slaViolationCount
) {
}
