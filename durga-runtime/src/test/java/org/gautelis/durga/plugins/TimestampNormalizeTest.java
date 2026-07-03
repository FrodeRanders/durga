package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimestampNormalizeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldNormalizeEpochMsToIso8601() throws Exception {
        System.out.println("TC: converts epoch milliseconds to ISO 8601 string");
        String result = TimestampNormalize.normalize(
                "{\"created_at\":1700000000000}", "created_at",
                "epoch_ms", "ISO8601", "UTC", false);
        assertTrue(result.contains("\"created_at\":\"20"));
    }

    @Test
    public void shouldNormalizeEpochSToEpochMs() throws Exception {
        System.out.println("TC: converts epoch seconds to epoch milliseconds");
        String result = TimestampNormalize.normalize(
                "{\"ts\":1700000000}", "ts",
                "epoch_s", "epoch_ms", "UTC", false);
        JsonNode node = mapper.readTree(result);
        assertEquals("1700000000000", node.get("ts").asText());
    }

    @Test
    public void shouldNormalizeIso8601ToEpochMs() throws Exception {
        System.out.println("TC: converts ISO 8601 string to epoch milliseconds");
        String result = TimestampNormalize.normalize(
                "{\"timestamp\":\"2023-11-15T00:00:00Z\"}", "timestamp",
                "ISO8601", "epoch_ms", "UTC", false);
        JsonNode node = mapper.readTree(result);
        long ms = Long.parseLong(node.get("timestamp").asText());
        assertTrue(ms > 1700000000000L);
    }

    @Test
    public void shouldNormalizeMultipleFields() throws Exception {
        System.out.println("TC: normalizes multiple timestamp fields at once");
        String result = TimestampNormalize.normalize(
                "{\"start\":1000000,\"end\":2000000}", "start,end",
                "epoch_s", "ISO8601", "UTC", false);
        assertTrue(result.contains("\"start\":\"19"));
        assertTrue(result.contains("\"end\":\"19"));
    }

    @Test
    public void shouldRemoveOnError() throws Exception {
        System.out.println("TC: removes field when parse fails and removeOnError is true");
        String result = TimestampNormalize.normalize(
                "{\"ts\":\"not-a-date\"}", "ts",
                "epoch_ms", "ISO8601", "UTC", true);
        assertFalse(result.contains("\"ts\""));
    }

    @Test
    public void shouldPassthroughOnBlankFields() {
        System.out.println("TC: returns input unchanged when fields config is blank");
        String input = "{\"a\":1}";
        assertEquals(input, TimestampNormalize.normalize(input, null, "epoch_ms", "ISO8601", "UTC", false));
        assertEquals(input, TimestampNormalize.normalize(input, "", "epoch_ms", "ISO8601", "UTC", false));
    }

    @Test
    public void shouldHandleNestedField() throws Exception {
        System.out.println("TC: normalizes nested timestamp field");
        String result = TimestampNormalize.normalize(
                "{\"meta\":{\"created_at\":1700000000000}}", "meta.created_at",
                "epoch_ms", "ISO8601", "UTC", false);
        assertTrue(result.contains("\"created_at\":\"2023"));
    }

    @Test
    public void shouldKeepFieldOnErrorByDefault() throws Exception {
        System.out.println("TC: preserves original value when parse fails and removeOnError is false");
        String input = "{\"ts\":\"garbage\"}";
        String result = TimestampNormalize.normalize(input, "ts", "epoch_ms", "ISO8601", "UTC", false);
        assertTrue(result.contains("\"ts\":\"garbage\""));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses fields, from and to config");
        Plugin plugin = new TimestampNormalize();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"timestamp\":1700000000000}"),
                "fields=timestamp;from=epoch_ms;to=ISO8601");
        JsonNode node = mapper.readTree(Plugin.toString(result));
        assertEquals("2023-11-14T22:13:20Z", node.get("timestamp").asText());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        TimestampNormalize.normalize("not json", "ts", "epoch_ms", "ISO8601", "UTC", false);
    }
}
