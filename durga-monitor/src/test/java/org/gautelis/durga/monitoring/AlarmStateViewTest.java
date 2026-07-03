package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlarmStateViewTest {

    @Test
    public void shouldBuildStateFromAlarmEvent() {
        System.out.println("TC: alarm state view is initialized from alarm event");

        AlarmEvent event = alarm("a-1", "cfg-1", AlarmSeverity.WARN,
                "pi-1", "approve", "2026-07-03T10:00:00Z", 4, "Too many failures");

        AlarmStateView view = AlarmStateView.fromEvent(event);

        assertEquals("order:approve:cfg-1", view.key());
        assertEquals(AlarmStateView.ACTIVE, view.status());
        assertEquals(1L, view.fireCount());
        assertEquals("2026-07-03T10:00:00Z", view.firstTriggeredAt());
        assertEquals("2026-07-03T10:00:00Z", view.lastTriggeredAt());
        assertEquals("Too many failures", view.lastMessage());
    }

    @Test
    public void shouldMergeRepeatedAlarmEventsAndKeepLatestDetails() {
        System.out.println("TC: alarm state view merges repeated firings and keeps latest details");

        AlarmStateView first = AlarmStateView.fromEvent(alarm("a-1", "cfg-1", AlarmSeverity.WARN,
                "pi-1", "approve", "2026-07-03T10:00:00Z", 4, "First"));
        AlarmStateView second = AlarmStateView.fromEvent(alarm("a-2", "cfg-1", AlarmSeverity.CRITICAL,
                "pi-2", "approve", "2026-07-03T10:05:00Z", 5, "Second"));

        AlarmStateView merged = first.merge(second);

        assertEquals(2L, merged.fireCount());
        assertEquals(AlarmSeverity.CRITICAL, merged.severity());
        assertEquals("2026-07-03T10:00:00Z", merged.firstTriggeredAt());
        assertEquals("2026-07-03T10:05:00Z", merged.lastTriggeredAt());
        assertEquals("pi-2", merged.lastProcessInstanceId());
        assertEquals("a-2", merged.lastAlarmId());
        assertEquals("Second", merged.lastMessage());
        assertEquals(5, merged.lastCount());
    }

    @Test
    public void shouldNotLetLateOlderEventOverwriteLatestDetails() {
        System.out.println("TC: alarm state view preserves latest details when older event arrives late");

        AlarmStateView newest = AlarmStateView.fromEvent(alarm("a-2", "cfg-1", AlarmSeverity.CRITICAL,
                "pi-2", "approve", "2026-07-03T10:05:00Z", 5, "Newest"));
        AlarmStateView older = AlarmStateView.fromEvent(alarm("a-1", "cfg-1", AlarmSeverity.WARN,
                "pi-1", "approve", "2026-07-03T10:00:00Z", 4, "Older"));

        AlarmStateView merged = newest.merge(older);

        assertEquals(2L, merged.fireCount());
        assertEquals(AlarmSeverity.CRITICAL, merged.severity());
        assertEquals("2026-07-03T10:05:00Z", merged.lastTriggeredAt());
        assertEquals("pi-2", merged.lastProcessInstanceId());
        assertEquals("a-2", merged.lastAlarmId());
        assertEquals("Newest", merged.lastMessage());
        assertEquals(5, merged.lastCount());
    }

    @Test
    public void shouldUseWildcardActivityForProcessWideAlarms() {
        System.out.println("TC: alarm state key uses wildcard for process-wide alarm");

        AlarmEvent event = alarm("a-1", "cfg-process", AlarmSeverity.WARN,
                "pi-1", null, "2026-07-03T10:00:00Z", 1, "Process failed");

        assertEquals("order:*:cfg-process", AlarmStateView.keyFor(event));
    }

    private static AlarmEvent alarm(
            String alarmId,
            String configId,
            AlarmSeverity severity,
            String instanceId,
            String activityId,
            String timestamp,
            int count,
            String message
    ) {
        return new AlarmEvent(
                alarmId,
                configId,
                AlarmSyndrome.COUNTED,
                severity,
                message,
                "order",
                instanceId,
                activityId,
                ProcessEvent.EventType.PROCESS_FAILED,
                timestamp,
                count,
                3
        );
    }
}
