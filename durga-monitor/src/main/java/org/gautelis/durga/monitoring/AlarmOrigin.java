package org.gautelis.durga.monitoring;

/**
 * Provenance layer of an {@link AlarmConfig}, expressing how it came to exist.
 * <p>
 * The layering forms a precedence order (highest first): {@link #EXPLICIT} &gt;
 * {@link #OPT_IN} &gt; {@link #AUTOMATIC}. A higher layer may refine or suppress a
 * lower-layer default for the same scope.
 */
public enum AlarmOrigin {
    /** Built into the monitor and always active, requiring no process configuration. */
    AUTOMATIC,

    /** A monitor-provided default that a process enables and optionally tunes. */
    OPT_IN,

    /** Fully specified by a process, typically via BPMN extension properties. */
    EXPLICIT
}
