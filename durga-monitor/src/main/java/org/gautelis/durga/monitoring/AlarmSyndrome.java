package org.gautelis.durga.monitoring;

/**
 * Fault detection syndromes for classifying observed behaviour patterns.
 */
public enum AlarmSyndrome {
    /** First occurrence triggers alarm immediately. */
    HARD_ERROR,

    /** Count occurrences; alarm when count exceeds threshold. */
    COUNTED,

    /** Count occurrences within a sliding time window; alarm when count exceeds threshold. */
    SLIDING_WINDOW,

    /**
     * Absence of progress: an active instance that has emitted no lifecycle event for longer
     * than the configured idle timeout ({@code windowDuration}) is reported as stuck.
     * Evaluated by a wall-clock punctuator rather than per incoming event.
     */
    STUCK,

    /**
     * System-wide surge: fires once when the number of instances that newly became
     * {@link #STUCK} within a rolling {@code windowDuration} exceeds the configured threshold.
     * Collapses a flood of individual stall alarms into a single higher-level alert.
     */
    CASCADE,

    /**
     * Performance SLA on wall-clock latency: fires when a completed unit of work takes
     * longer than the agreed maximum duration. Scope is either a single task (activity
     * entry to completion) or the whole process (process start to completion). The maximum
     * allowed latency is carried in {@code windowDuration}. Evaluated per completing
     * instance, so each breaching completion produces one alarm.
     */
    SLA_LATENCY,

    /**
     * Performance SLA on throughput: fires when the number of completed units of work in
     * the trailing {@code windowDuration} falls below the agreed minimum ({@code threshold}
     * = minimum calls per period). Scope is a single task or the whole process. Evaluated by
     * a wall-clock punctuator once a full window has elapsed since the first observation, and
     * re-arms when throughput recovers to or above the minimum.
     */
    SLA_THROUGHPUT
}
