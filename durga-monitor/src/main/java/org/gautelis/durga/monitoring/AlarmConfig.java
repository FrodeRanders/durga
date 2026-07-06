package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for a fault detection alarm.
 * <p>
 * Matching logic:
 * <ul>
 *   <li>If both {@code processId} and {@code activityId} are non-null: matches that
 *       specific activity in that specific process.</li>
 *   <li>If only {@code processId} is non-null: matches any activity in that process.</li>
 *   <li>If only {@code activityId} is non-null: matches that activity in any process.</li>
 *   <li>If both are null: matches every event (caution — use with narrow
 *       {@code eventType}).</li>
 * </ul>
 *
 * @param id           unique configuration identifier
 * @param processId    scope: process to monitor (null = all processes)
 * @param activityId   scope: activity to monitor (null = all activities)
 * @param eventType    which lifecycle event type triggers counting (may be null for
 *                     {@link AlarmSyndrome#STUCK}, {@link AlarmSyndrome#CASCADE}, and the
 *                     SLA syndromes, which either use a punctuator or default the event type
 *                     from the scope)
 * @param syndrome     how faults are aggregated into an alarm
 * @param threshold    maximum count before alarm (ignored for {@link AlarmSyndrome#HARD_ERROR}
 *                     and {@link AlarmSyndrome#STUCK}); for {@link AlarmSyndrome#SLA_THROUGHPUT}
 *                     it is the <em>minimum</em> required calls per {@code windowDuration}
 * @param windowDuration size of sliding window for {@link AlarmSyndrome#SLIDING_WINDOW} /
 *                     {@link AlarmSyndrome#CASCADE}, idle timeout for {@link AlarmSyndrome#STUCK},
 *                     the maximum allowed wall-clock latency for {@link AlarmSyndrome#SLA_LATENCY},
 *                     or the measurement period for {@link AlarmSyndrome#SLA_THROUGHPUT}
 * @param severity     alarm severity level
 * @param message      human-readable alarm message template, may contain
 *                     {@code ${processId}}, {@code ${activityId}}, {@code ${count}},
 *                     {@code ${processInstanceId}}, {@code ${idleSeconds}},
 *                     {@code ${latencyMs}}, {@code ${limitMs}}, {@code ${windowSeconds}}
 * @param origin       provenance layer (automatic / opt-in / explicit)
 */
public record AlarmConfig(
        String id,
        String processId,
        String activityId,
        ProcessEvent.EventType eventType,
        AlarmSyndrome syndrome,
        int threshold,
        Duration windowDuration,
        AlarmSeverity severity,
        String message,
        AlarmOrigin origin
) {
    public AlarmConfig {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(syndrome, "syndrome");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(origin, "origin");

        switch (syndrome) {
            case HARD_ERROR, COUNTED, SLIDING_WINDOW -> Objects.requireNonNull(eventType, "eventType");
            default -> { /* STUCK / CASCADE / SLA_* default or ignore eventType */ }
        }
        if (syndrome == AlarmSyndrome.SLIDING_WINDOW && windowDuration == null) {
            throw new IllegalArgumentException("SLIDING_WINDOW requires windowDuration");
        }
        if (syndrome == AlarmSyndrome.STUCK && windowDuration == null) {
            throw new IllegalArgumentException("STUCK requires windowDuration (idle timeout)");
        }
        if (syndrome == AlarmSyndrome.CASCADE && windowDuration == null) {
            throw new IllegalArgumentException("CASCADE requires windowDuration");
        }
        if (syndrome == AlarmSyndrome.SLA_LATENCY && windowDuration == null) {
            throw new IllegalArgumentException("SLA_LATENCY requires windowDuration (maximum allowed latency)");
        }
        if (syndrome == AlarmSyndrome.SLA_THROUGHPUT && windowDuration == null) {
            throw new IllegalArgumentException("SLA_THROUGHPUT requires windowDuration (measurement period)");
        }
        boolean needsThreshold = syndrome == AlarmSyndrome.COUNTED
                || syndrome == AlarmSyndrome.SLIDING_WINDOW
                || syndrome == AlarmSyndrome.CASCADE
                || syndrome == AlarmSyndrome.SLA_THROUGHPUT;
        if (needsThreshold && threshold <= 0) {
            throw new IllegalArgumentException(syndrome + " requires threshold > 0");
        }
    }

    /**
     * Returns whether this config matches the given event's process and activity.
     */
    public boolean matches(String eventProcessId, String eventActivityId) {
        if (processId != null && !processId.equals(eventProcessId)) return false;
        if (activityId != null && !activityId.equals(eventActivityId)) return false;
        return true;
    }
}
