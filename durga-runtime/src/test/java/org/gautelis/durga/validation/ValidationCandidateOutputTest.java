package org.gautelis.durga.validation;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class ValidationCandidateOutputTest {

    @Test
    public void shouldRoundTripThroughJson() {
        System.out.println("TC: ValidationCandidateOutput survives JSON round-trip");
        ValidationCandidateOutput output = new ValidationCandidateOutput(
                "order_fulfillment", "validate_order", "inst-1", "validate_order",
                "tok-1", "corr-1", "biz-1", "v2",
                Map.of("amount", 100), Map.of("amount", 100, "valid", true),
                "PAYLOAD", null, "idem-1", null, null, "2026-01-01T00:00:00Z");

        ValidationCandidateOutput parsed = ValidationCandidateOutput.fromJson(output.toJson());
        assertEquals(output, parsed);
    }

    @Test
    public void shouldDefaultTimestampWhenNull() {
        System.out.println("TC: null timestamp defaults to now");
        ValidationCandidateOutput output = new ValidationCandidateOutput(
                "p", "t", "i", "t", null, null, null, "v1",
                Map.of(), Map.of(), "PAYLOAD", null, "idem", null, null, null);
        assertNotNull(output.timestamp());
    }

    @Test
    public void shouldBuildStableKey() {
        System.out.println("TC: key is processId:taskId:processInstanceId and matches static builder");
        ValidationCandidateOutput output = new ValidationCandidateOutput(
                "p", "t", "i", "t", null, null, null, "v1",
                Map.of(), Map.of(), "PAYLOAD", null, "idem", null, null, "2026-01-01T00:00:00Z");
        assertEquals("p:t:i", output.key());
        assertEquals(output.key(), ValidationCandidateOutput.key("p", "t", "i"));
    }

    @Test
    public void shouldCarryErrorInfo() {
        System.out.println("TC: candidate error info survives JSON round-trip");
        ValidationCandidateOutput output = new ValidationCandidateOutput(
                "p", "t", "i", "t", null, null, null, "v2",
                Map.of("in", 1), null, "PAYLOAD", "boom", "idem", "FAIL",
                new ProcessEvent.ErrorInfo("boom", "PLUGIN_FAILED"), "2026-01-01T00:00:00Z");
        ValidationCandidateOutput parsed = ValidationCandidateOutput.fromJson(output.toJson());
        assertEquals("FAIL", parsed.errorStrategy());
        assertEquals("PLUGIN_FAILED", parsed.error().code());
        assertEquals("boom", parsed.error().message());
    }
}
