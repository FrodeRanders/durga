package org.gautelis.durga.monitoring;

import java.util.Arrays;

record ActivityLatencyStats(
        String processId,
        String activityId,
        long sampleCount,
        long totalDurationMs,
        long maxDurationMs,
        long slaViolationCount,
        long[] recentSamples
) {
    private static final int MAX_SAMPLES = 100;

    static ActivityLatencyStats empty() {
        return new ActivityLatencyStats(null, null, 0L, 0L, 0L, 0L, null);
    }

    ActivityLatencyStats add(ActivityLatencyDelta sample) {
        long duration = sample.durationMs();
        long newCount = sampleCount + 1;
        long newTotal = totalDurationMs + duration;
        long newMax = Math.max(maxDurationMs, duration);
        long newViolations = slaViolationCount + (sample.slaViolation() ? 1 : 0);

        long[] samples = recentSamples != null
                ? Arrays.copyOf(recentSamples, (int) Math.min(MAX_SAMPLES, sampleCount + 1))
                : new long[1];

        int insertPos = Arrays.binarySearch(samples, 0,
                Math.min(samples.length, (int) sampleCount), duration);
        if (insertPos < 0) {
            insertPos = -(insertPos + 1);
        }

        if (sampleCount < MAX_SAMPLES) {
            if (newCount > samples.length) {
                samples = Arrays.copyOf(samples, (int) newCount);
            }
            int moveCount = (int) sampleCount - insertPos;
            if (moveCount > 0) {
                System.arraycopy(samples, insertPos, samples, insertPos + 1, moveCount);
            }
            samples[insertPos] = duration;
        } else {
            if (insertPos < MAX_SAMPLES) {
                int moveCount = MAX_SAMPLES - 1 - insertPos;
                if (moveCount > 0) {
                    System.arraycopy(samples, insertPos, samples, insertPos + 1, moveCount);
                }
                samples[insertPos] = duration;
            }
        }

        return new ActivityLatencyStats(
                sample.processId(),
                sample.activityId(),
                newCount, newTotal, newMax, newViolations, samples
        );
    }

    ActivityLatencyStats withSlaThreshold(long thresholdMs) {
        long newViolations = slaViolationCount;
        if (sampleCount > 0) {
            for (int i = 0; i < Math.min(sampleCount, recentSamples != null ? recentSamples.length : 0); i++) {
                if (recentSamples[i] > thresholdMs) {
                    newViolations++;
                }
            }
        }
        return new ActivityLatencyStats(
                processId, activityId, sampleCount, totalDurationMs, maxDurationMs, newViolations, recentSamples
        );
    }

    long percentile(double pct) {
        if (recentSamples == null || sampleCount == 0) {
            return 0L;
        }
        int size = (int) Math.min(sampleCount, recentSamples.length);
        if (size == 0) {
            return 0L;
        }
        int idx = (int) Math.round(pct / 100.0 * (size - 1));
        return recentSamples[Math.min(idx, size - 1)];
    }

    ActivityLatencySummary toSummary() {
        long average = sampleCount == 0 ? 0L : totalDurationMs / sampleCount;
        long p50 = percentile(50.0);
        long p95 = percentile(95.0);
        long p99 = percentile(99.0);
        return new ActivityLatencySummary(
                processId, activityId, sampleCount, average, maxDurationMs,
                p50, p95, p99, slaViolationCount
        );
    }
}
