package org.gautelis.durga.monitoring;

record ActivityLatencyStats(
        String processId,
        String activityId,
        long sampleCount,
        long totalDurationMs,
        long maxDurationMs
) {
    static ActivityLatencyStats empty() {
        return new ActivityLatencyStats(null, null, 0L, 0L, 0L);
    }

    ActivityLatencyStats add(ActivityLatencyDelta sample) {
        return new ActivityLatencyStats(
                sample.processId(),
                sample.activityId(),
                sampleCount + 1,
                totalDurationMs + sample.durationMs(),
                Math.max(maxDurationMs, sample.durationMs())
        );
    }

    ActivityLatencySummary toSummary() {
        long average = sampleCount == 0 ? 0L : totalDurationMs / sampleCount;
        return new ActivityLatencySummary(processId, activityId, sampleCount, average, maxDurationMs);
    }
}
