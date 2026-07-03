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
 * @param eventType    which lifecycle event type triggers counting
 * @param syndrome     how faults are aggregated into an alarm
 * @param threshold    maximum count before alarm (ignored for {@link AlarmSyndrome#HARD_ERROR})
 * @param windowDuration size of sliding window (only for {@link AlarmSyndrome#SLIDING_WINDOW})
 * @param severity     alarm severity level
 * @param message      human-readable alarm message template, may contain
 *                     {@code ${processId}}, {@code ${activityId}}, {@code ${count}},
 *                     {@code ${processInstanceId}}
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
        String message
) {
    public AlarmConfig {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(syndrome, "syndrome");
        Objects.requireNonNull(severity, "severity");
        if (syndrome == AlarmSyndrome.SLIDING_WINDOW && windowDuration == null) {
            throw new IllegalArgumentException("SLIDING_WINDOW requires windowDuration");
        }
        if (syndrome != AlarmSyndrome.HARD_ERROR && threshold <= 0) {
            throw new IllegalArgumentException("COUNTED and SLIDING_WINDOW require threshold > 0");
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
