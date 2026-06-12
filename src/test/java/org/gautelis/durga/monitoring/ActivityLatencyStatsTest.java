package org.gautelis.durga.monitoring;

import org.junit.Test;

import static org.junit.Assert.*;

public class ActivityLatencyStatsTest {

    @Test
    public void shouldTrackSampleCount() {
        System.out.println("TC: tracks sample count and total duration sum across added deltas");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 100, false));
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 200, false));
        assertEquals(2L, stats.sampleCount());
        assertEquals(300L, stats.totalDurationMs());
    }

    @Test
    public void shouldTrackMax() {
        System.out.println("TC: tracks maximum duration across all added deltas");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 100, false));
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 500, false));
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 300, false));
        assertEquals(500L, stats.maxDurationMs());
    }

    @Test
    public void shouldComputePercentiles() {
        System.out.println("TC: computes p50, p95 and p99 percentiles from sorted samples");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        for (int i = 1; i <= 50; i++) {
            stats = stats.add(new ActivityLatencyDelta("p1", "a1", i * 10, false));
        }
        long p50 = stats.percentile(50.0);
        long p95 = stats.percentile(95.0);
        long p99 = stats.percentile(99.0);
        assertTrue("p50 should be around 250", p50 >= 240 && p50 <= 260);
        assertTrue("p95 should be around 475", p95 >= 460 && p95 <= 500);
        assertTrue("p99 should be around 500", p99 >= 480 && p99 <= 500);
    }

    @Test
    public void shouldTrackSlaViolations() {
        System.out.println("TC: counts samples exceeding SLA threshold as violations");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        stats = stats.add(ActivityLatencyDelta.of("p1", "a1", 100, 50));
        stats = stats.add(ActivityLatencyDelta.of("p1", "a1", 200, 50));
        stats = stats.add(ActivityLatencyDelta.of("p1", "a1", 30, 50));
        assertEquals(2L, stats.slaViolationCount());
    }

    @Test
    public void shouldNotCountViolationsWhenThresholdZero() {
        System.out.println("TC: counts no SLA violations when threshold is zero");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        stats = stats.add(ActivityLatencyDelta.of("p1", "a1", 999999, 0));
        assertEquals(0L, stats.slaViolationCount());
    }

    @Test
    public void shouldIncludePercentilesInSummary() {
        System.out.println("TC: ActivityLatencySummary includes percentiles, average, max and violation count");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        for (int i = 1; i <= 10; i++) {
            stats = stats.add(new ActivityLatencyDelta("p1", "a1", i * 100, false));
        }
        ActivityLatencySummary summary = stats.toSummary();
        assertEquals(10L, summary.sampleCount());
        assertEquals(550L, summary.averageDurationMs());
        assertEquals(1000L, summary.maxDurationMs());
        assertTrue(summary.p50DurationMs() > 0);
        assertTrue(summary.p95DurationMs() > 0);
        assertTrue(summary.p99DurationMs() > 0);
        assertEquals(0L, summary.slaViolationCount());
    }

    @Test
    public void percentileShouldWorkWithSingleSample() {
        System.out.println("TC: percentile returns the single sample value for any percentile with one sample");
        ActivityLatencyStats stats = ActivityLatencyStats.empty();
        stats = stats.add(new ActivityLatencyDelta("p1", "a1", 42, false));
        assertEquals(42L, stats.percentile(50.0));
        assertEquals(42L, stats.percentile(99.0));
    }
}
