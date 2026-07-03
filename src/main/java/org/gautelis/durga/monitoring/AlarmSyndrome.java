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
    SLIDING_WINDOW
}
