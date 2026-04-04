package org.gautelis.durga.monitoring;

record ActivityLatencyDelta(
        String processId,
        String activityId,
        long durationMs
) {
    String key() {
        return processId + ":" + activityId;
    }
}
