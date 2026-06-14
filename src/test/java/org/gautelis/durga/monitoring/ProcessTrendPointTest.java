package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcessTrendPointTest {
    @Test
    public void shouldParseTrendKeyWithIsoTimestampColons() {
        System.out.println("TC: parses trend key preserving ISO timestamp and lifecycle metric");
        ProcessEvent event = new ProcessEvent(
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
                "2026-04-03T08:01:45Z"
        );

        ProcessTrendPoint point = ProcessTrendPoint.fromKey(ProcessTrendPoint.keyFor(event), 3L);

        assertEquals("invoice_receipt", point.processId());
        assertEquals("2026-04-03T08:01:00Z", point.bucketStartedAt());
        assertEquals("started", point.metric());
        assertEquals(3L, point.count());
    }
}
