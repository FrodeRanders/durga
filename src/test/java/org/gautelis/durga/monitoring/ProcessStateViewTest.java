package org.gautelis.durga.monitoring;

import org.junit.Test;
import org.gautelis.durga.ProcessEvent;

import java.util.Map;

import static org.junit.Assert.*;

public class ProcessStateViewTest {
    @Test
    public void shouldProjectLatestStateAndDurations() {
        System.out.println("TC: projects latest state, current activity, and activity durations from event sequence");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-1",
                        "invoice_receipt",
                        "start",
                        "token-1",
                        "corr-1",
                        Map.of(),
                        ProcessEvent.Status.STARTED,
                        null,
                        ProcessEvent.EventType.PROCESS_STARTED,
                        "v1",
                        "INV-1",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-1",
                        "invoice_receipt",
                        "review_invoice",
                        "token-1",
                        "corr-1",
                        Map.of(),
                        ProcessEvent.Status.STARTED,
                        null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED,
                        "v1",
                        "INV-1",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-1",
                        "invoice_receipt",
                        "review_invoice",
                        "token-1",
                        "corr-1",
                        Map.of(),
                        ProcessEvent.Status.COMPLETED,
                        null,
                        ProcessEvent.EventType.ACTIVITY_COMPLETED,
                        "v1",
                        "INV-1",
                        "2026-04-03T08:03:00Z"
                ));

        assertEquals("invoice_receipt", state.processId());
        assertEquals("review_invoice", state.currentActivityId());
        assertEquals("active", state.lifecycleState());
        assertEquals("invoice_receipt:review_invoice", state.currentStateKey());
        assertEquals(Long.valueOf(120000L), state.activityDurationsMs().get("review_invoice"));
    }

    @Test
    public void shouldTrackFailuresAsRetries() {
        System.out.println("TC: tracks process failure as lifecycle failure state with retry count and error code");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-2",
                        "invoice_receipt",
                        "review_invoice",
                        "token-2",
                        "corr-2",
                        Map.of(),
                        ProcessEvent.Status.FAILED,
                        new ProcessEvent.ErrorInfo("boom", "TASK_FAILED"),
                        ProcessEvent.EventType.PROCESS_FAILED,
                        "v1",
                        "INV-2",
                        "2026-04-03T08:05:00Z"
                ));

        assertEquals("failed", state.lifecycleState());
        assertEquals(1, state.retryCount());
        assertEquals("TASK_FAILED", state.lastErrorCode());
    }

    @Test
    public void shouldHandleActivityCancelled() {
        System.out.println("TC: sets lifecycle to cancelled and records duration when activity is cancelled");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-3", "order_proc", "start", "token-3", "corr-3",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-3",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-3", "order_proc", "validate_order", "token-3", "corr-3",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-3",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-3", "order_proc", "validate_order", "token-3", "corr-3",
                        Map.of(), ProcessEvent.Status.CANCELLED, null,
                        ProcessEvent.EventType.ACTIVITY_CANCELLED, "v1", "ORD-3",
                        "2026-04-03T08:02:00Z"
                ));

        assertEquals("cancelled", state.lifecycleState());
        assertTrue(state.activityDurationsMs().containsKey("validate_order"));
    }

    @Test
    public void shouldHandleActivityEscalated() {
        System.out.println("TC: keeps lifecycle active and records duration when activity escalates");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-4", "order_proc", "start", "token-4", "corr-4",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-4",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-4", "order_proc", "approve_order", "token-4", "corr-4",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-4",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-4", "order_proc", "approve_order", "token-4", "corr-4",
                        Map.of(), ProcessEvent.Status.ESCALATED, null,
                        ProcessEvent.EventType.ACTIVITY_ESCALATED, "v1", "ORD-4",
                        "2026-04-03T08:05:00Z"
                ));

        assertEquals("active", state.lifecycleState());
        assertNotNull(state.activityDurationsMs().get("approve_order"));
    }

    @Test
    public void shouldHandleProcessCompletedWithDuration() {
        System.out.println("TC: sets lifecycle to completed, records completedAt and activity duration");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-5", "invoice_receipt", "start", "token-5", "corr-5",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "INV-5",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-5", "invoice_receipt", "final_task", "token-5", "corr-5",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "INV-5",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-5", "invoice_receipt", "final_task", "token-5", "corr-5",
                        Map.of(), ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.PROCESS_COMPLETED, "v1", "INV-5",
                        "2026-04-03T08:10:00Z"
                ));

        assertEquals("completed", state.lifecycleState());
        assertEquals("completed", state.currentActivityId());
        assertNotNull(state.completedAt());
        assertTrue(state.activityDurationsMs().containsKey("final_task"));
    }

    @Test
    public void shouldHandleGatewayTaken() {
        System.out.println("TC: keeps lifecycle active and records duration on gateway_taken event");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-6", "order_proc", "start", "token-6", "corr-6",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-6",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-6", "order_proc", "check_stock", "token-6", "corr-6",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-6",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-6", "order_proc", "check_stock", "token-6", "corr-6",
                        Map.of(), ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.GATEWAY_TAKEN, "v1", "ORD-6",
                        "2026-04-03T08:02:00Z"
                ));

        assertEquals("active", state.lifecycleState());
        assertTrue(state.activityDurationsMs().containsKey("check_stock"));
    }

    @Test
    public void shouldHandleNullActivityIdGracefully() {
        System.out.println("TC: handles null activityId without crashing and produces empty durations");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-7", "order_proc", null, "token-7", "corr-7",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-7",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-7", "order_proc", null, "token-7", "corr-7",
                        Map.of(), ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", "ORD-7",
                        "2026-04-03T08:01:00Z"
                ));

        assertEquals("active", state.lifecycleState());
        assertTrue(state.activityDurationsMs().isEmpty());
    }

    @Test
    public void shouldPreserveStartedAtAcrossEvents() {
        System.out.println("TC: preserves startedAt timestamp while updating lastUpdatedAt on subsequent events");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-8", "order_proc", "start", "token-8", "corr-8",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-8",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-8", "order_proc", "task-1", "token-8", "corr-8",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-8",
                        "2026-04-03T08:05:00Z"
                ));

        assertEquals("2026-04-03T08:00:00Z", state.startedAt());
        assertEquals("2026-04-03T08:05:00Z", state.lastUpdatedAt());
    }

    @Test
    public void emptyShouldReturnSaneDefaults() {
        System.out.println("TC: empty state returns null instance id, unknown state, and empty collections");
        ProcessStateView empty = ProcessStateView.empty();

        assertNull(empty.processInstanceId());
        assertEquals(ProcessStateView.UNKNOWN_STATE, empty.lifecycleState());
        assertEquals(0, empty.retryCount());
        assertTrue(empty.activityEnteredAt().isEmpty());
        assertTrue(empty.activityDurationsMs().isEmpty());
        assertNull(empty.lastErrorCode());
        assertNull(empty.lastErrorMessage());
    }

    @Test
    public void currentStateKeyShouldHandleNulls() {
        System.out.println("TC: currentStateKey returns unknown:unknown for empty state and process:activity after apply");
        ProcessStateView state = ProcessStateView.empty();
        ProcessStateView withPid = state
                .apply(new ProcessEvent(
                        "pi-9", "test_process", "start", "token-9", "corr-9",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", null,
                        "2026-04-03T08:00:00Z"
                ));

        assertEquals("unknown:unknown", state.currentStateKey());
        assertEquals("test_process:start", withPid.currentStateKey());
    }

    @Test
    public void shouldTrackMultipleConcurrentActivities() {
        System.out.println("TC: tracks durations for multiple concurrent activities independently");
        ProcessStateView state = ProcessStateView.empty()
                .apply(new ProcessEvent(
                        "pi-10", "order_proc", "start", "token-10", "corr-10",
                        Map.of(), ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", "ORD-10",
                        "2026-04-03T08:00:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-10", "order_proc", "task-a", "token-a", "corr-10",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-10",
                        "2026-04-03T08:01:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-10", "order_proc", "task-b", "token-b", "corr-10",
                        Map.of(), null, null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", "ORD-10",
                        "2026-04-03T08:02:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-10", "order_proc", "task-a", "token-a", "corr-10",
                        Map.of(), ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", "ORD-10",
                        "2026-04-03T08:03:00Z"
                ))
                .apply(new ProcessEvent(
                        "pi-10", "order_proc", "task-b", "token-b", "corr-10",
                        Map.of(), ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", "ORD-10",
                        "2026-04-03T08:05:00Z"
                ));

        assertTrue(state.activityDurationsMs().containsKey("task-a"));
        assertTrue(state.activityDurationsMs().containsKey("task-b"));
        assertEquals(Long.valueOf(120000L), state.activityDurationsMs().get("task-a"));
        assertEquals(Long.valueOf(180000L), state.activityDurationsMs().get("task-b"));
    }
}
