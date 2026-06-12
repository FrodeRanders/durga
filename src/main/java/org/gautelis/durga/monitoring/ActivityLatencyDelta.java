package org.gautelis.durga.monitoring;

record ActivityLatencyDelta(
        String processId,
        String activityId,
        long durationMs,
        boolean slaViolation
) {
    String key() {
        return processId + ":" + activityId;
    }

    static ActivityLatencyDelta of(String processId, String activityId, long durationMs, long slaThresholdMs) {
        return new ActivityLatencyDelta(processId, activityId, durationMs,
                slaThresholdMs > 0 && durationMs > slaThresholdMs);
    }
}
