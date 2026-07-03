package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;

/**
 * Materialized operational state for one alarm configuration and scope.
 *
 * @param key stable state key
 * @param status current alarm status
 * @param configId triggering alarm config
 * @param syndrome detection syndrome
 * @param severity current severity
 * @param processId process where the alarm applies
 * @param activityId activity where the alarm applies, or null for process-wide
 * @param lastProcessInstanceId last process instance that fired the alarm
 * @param triggerEventType event type that last fired the alarm
 * @param firstTriggeredAt first observed firing timestamp
 * @param lastTriggeredAt latest observed firing timestamp
 * @param fireCount number of firings folded into this state
 * @param lastCount latest detector count
 * @param threshold configured threshold
 * @param lastAlarmId latest alarm event id
 * @param lastMessage latest rendered alarm message
 */
public record AlarmStateView(
        String key,
        String status,
        String configId,
        AlarmSyndrome syndrome,
        AlarmSeverity severity,
        String processId,
        String activityId,
        String lastProcessInstanceId,
        ProcessEvent.EventType triggerEventType,
        String firstTriggeredAt,
        String lastTriggeredAt,
        long fireCount,
        int lastCount,
        int threshold,
        String lastAlarmId,
        String lastMessage
) {
    public static final String ACTIVE = "ACTIVE";

    public static String keyFor(AlarmEvent event) {
        String activity = event.activityId() != null && !event.activityId().isBlank()
                ? event.activityId() : "*";
        return event.processId() + ":" + activity + ":" + event.configId();
    }

    public static AlarmStateView fromEvent(AlarmEvent event) {
        return new AlarmStateView(
                keyFor(event),
                ACTIVE,
                event.configId(),
                event.syndrome(),
                event.severity(),
                event.processId(),
                event.activityId(),
                event.processInstanceId(),
                event.triggerEventType(),
                event.triggerTimestamp(),
                event.triggerTimestamp(),
                1L,
                event.count(),
                event.threshold(),
                event.alarmId(),
                event.message()
        );
    }

    public AlarmStateView merge(AlarmStateView next) {
        if (next == null) {
            return this;
        }
        AlarmStateView latest = isAtOrAfter(next.lastTriggeredAt, lastTriggeredAt) ? next : this;
        return new AlarmStateView(
                key,
                ACTIVE,
                configId,
                syndrome,
                maxSeverity(severity, next.severity),
                processId,
                activityId,
                latest.lastProcessInstanceId,
                latest.triggerEventType,
                earliest(firstTriggeredAt, next.firstTriggeredAt),
                latest(lastTriggeredAt, next.lastTriggeredAt),
                fireCount + next.fireCount,
                latest.lastCount,
                latest.threshold,
                latest.lastAlarmId,
                latest.lastMessage
        );
    }

    private static AlarmSeverity maxSeverity(AlarmSeverity left, AlarmSeverity right) {
        if (left == AlarmSeverity.CRITICAL || right == AlarmSeverity.CRITICAL) {
            return AlarmSeverity.CRITICAL;
        }
        return left != null ? left : right;
    }

    private static String earliest(String left, String right) {
        if (left == null || left.isBlank()) return right;
        if (right == null || right.isBlank()) return left;
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String latest(String left, String right) {
        if (left == null || left.isBlank()) return right;
        if (right == null || right.isBlank()) return left;
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static boolean isAtOrAfter(String candidate, String reference) {
        if (candidate == null || candidate.isBlank()) return false;
        if (reference == null || reference.isBlank()) return true;
        return candidate.compareTo(reference) >= 0;
    }
}
