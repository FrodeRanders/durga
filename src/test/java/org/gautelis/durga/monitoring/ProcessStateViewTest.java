package org.gautelis.durga.monitoring;

import org.junit.Test;
import org.gautelis.durga.ProcessEvent;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcessStateViewTest {
    @Test
    public void shouldProjectLatestStateAndDurations() {
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
}
