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
    CASCADE
}
