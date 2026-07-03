package org.gautelis.durga;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class ProcessEventTest {

    @Test
    public void shouldRoundTripThroughJsonWithFullConstructor() {
        System.out.println("TC: round-trips through JSON preserving all fields including payload and error info");
        ProcessEvent event = new ProcessEvent(
                "pi-1",
                "invoice_receipt",
                "review_invoice",
                "token-1",
                "corr-1",
                Map.of("amount", 100),
                ProcessEvent.Status.COMPLETED,
                new ProcessEvent.ErrorInfo("something went wrong", "TASK_FAILED"),
                ProcessEvent.EventType.ACTIVITY_COMPLETED,
                "v1",
                "INV-1",
                "2026-04-03T08:03:00Z"
        );

        String json = event.toJson();
        ProcessEvent parsed = ProcessEvent.fromJson(json);

        assertEquals("pi-1", parsed.processInstanceId());
        assertEquals("invoice_receipt", parsed.processId());
        assertEquals("review_invoice", parsed.activityId());
        assertEquals("token-1", parsed.tokenId());
        assertEquals("corr-1", parsed.correlationId());
        assertEquals(100, parsed.payload().get("amount"));
        assertEquals(ProcessEvent.EventType.ACTIVITY_COMPLETED, parsed.eventType());
        assertEquals("v1", parsed.processVersion());
        assertEquals("INV-1", parsed.businessKey());
        assertEquals("2026-04-03T08:03:00Z", parsed.timestamp());
        assertNotNull(parsed.error());
        assertEquals("TASK_FAILED", parsed.error().code());
        assertEquals("something went wrong", parsed.error().message());
    }

    @Test
    public void shouldRoundTripThroughJsonWithCompactConstructor() {
        System.out.println("TC: round-trips through JSON with compact constructor and inferred event type");
        ProcessEvent event = new ProcessEvent(
                "pi-2",
                "invoice_receipt",
                "validate_data",
                "token-2",
                "corr-2",
                Map.of(),
                ProcessEvent.Status.STARTED,
                null
        );

        String json = event.toJson();
        ProcessEvent parsed = ProcessEvent.fromJson(json);

        assertEquals("pi-2", parsed.processInstanceId());
        assertEquals("invoice_receipt", parsed.processId());
        assertEquals("validate_data", parsed.activityId());
        assertEquals(ProcessEvent.EventType.PROCESS_STARTED, parsed.eventType());
        assertNotNull(parsed.timestamp());
        assertNull(parsed.error());
        assertNull(parsed.processVersion());
        assertNull(parsed.businessKey());
    }

    @Test
    public void shouldInferEventTypeFromStatus() {
        System.out.println("TC: infers correct event type from status for all lifecycle states");
        ProcessEvent started = new ProcessEvent(
                "pi-3", "proc", "act", "tok", "corr", Map.of(), ProcessEvent.Status.STARTED, null);
        assertEquals(ProcessEvent.EventType.PROCESS_STARTED, started.eventType());

        ProcessEvent completed = new ProcessEvent(
                "pi-3", "proc", "act", "tok", "corr", Map.of(), ProcessEvent.Status.COMPLETED, null);
        assertEquals(ProcessEvent.EventType.ACTIVITY_COMPLETED, completed.eventType());

        ProcessEvent failed = new ProcessEvent(
                "pi-3", "proc", "act", "tok", "corr", Map.of(), ProcessEvent.Status.FAILED, null);
        assertEquals(ProcessEvent.EventType.PROCESS_FAILED, failed.eventType());

        ProcessEvent escalated = new ProcessEvent(
                "pi-3", "proc", "act", "tok", "corr", Map.of(), ProcessEvent.Status.ESCALATED, null);
        assertEquals(ProcessEvent.EventType.ACTIVITY_ESCALATED, escalated.eventType());

        ProcessEvent cancelled = new ProcessEvent(
                "pi-3", "proc", "act", "tok", "corr", Map.of(), ProcessEvent.Status.CANCELLED, null);
        assertEquals(ProcessEvent.EventType.ACTIVITY_CANCELLED, cancelled.eventType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws IllegalArgumentException when parsing invalid JSON");
        ProcessEvent.fromJson("{not valid json");
    }
}
