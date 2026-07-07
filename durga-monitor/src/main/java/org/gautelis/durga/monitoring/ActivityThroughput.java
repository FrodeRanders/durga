package org.gautelis.durga.monitoring;

/**
 * Per-activity throughput: how many items a task (or gateway) has processed within one process
 * definition. Derived by counting terminal per-activity lifecycle events
 * ({@code ACTIVITY_COMPLETED}, {@code ACTIVITY_ESCALATED}, {@code GATEWAY_TAKEN}), so it is available
 * for plugin/data-pipeline tasks that emit only completion events — unlike latency, which needs an
 * {@code ACTIVITY_ENTERED}/{@code ACTIVITY_COMPLETED} pair.
 *
 * @param processId process definition identifier
 * @param activityId activity identifier
 * @param count number of items processed by this activity
 */
public record ActivityThroughput(
        String processId,
        String activityId,
        long count
) {
    static ActivityThroughput empty() {
        return new ActivityThroughput(null, null, 0L);
    }

    ActivityThroughput increment(String processId, String activityId) {
        return new ActivityThroughput(processId, activityId, count + 1);
    }
}
