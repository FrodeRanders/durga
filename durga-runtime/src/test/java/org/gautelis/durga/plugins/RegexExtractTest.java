package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class RegexExtractTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldExtractNamedGroups() throws Exception {
        System.out.println("TC: extracts named capture groups from source field");
        String result = RegexExtract.extract(
                "{\"log\":\"192.168.1.1 GET /index.html 200\"}",
                "log",
                "(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(?<method>\\w+)",
                "parsed",
                false);
        assertTrue(result.contains("\"ip\":\"192.168.1.1\""));
        assertTrue(result.contains("\"method\":\"GET\""));
    }

    @Test
    public void shouldStoreInlineWithoutTarget() throws Exception {
        System.out.println("TC: stores extracted groups at top level when no target");
        String result = RegexExtract.extract(
                "{\"msg\":\"error: timeout after 30s\"}",
                "msg",
                "(?<severity>\\w+):\\s+(?<detail>.+)",
                null,
                false);
        assertTrue(result.contains("\"severity\":\"error\""));
        assertTrue(result.contains("\"detail\":\"timeout after 30s\""));
    }

    @Test
    public void shouldReturnInputOnNoMatch() {
        System.out.println("TC: returns input unchanged when regex does not match");
        String input = "{\"msg\":\"no match here\"}";
        String result = RegexExtract.extract(
                input, "msg", "(?<id>\\d+)", null, false);
        assertEquals(input, result);
    }

    @Test
    public void shouldReturnInputOnNullSourceOrPattern() {
        System.out.println("TC: returns input unchanged when source or pattern is null");
        String input = "{\"a\":1}";
        assertEquals(input, RegexExtract.extract(input, null, "pattern", null, false));
        assertEquals(input, RegexExtract.extract(input, "a", null, null, false));
    }

    @Test
    public void shouldHandleNumberedGroups() throws Exception {
        System.out.println("TC: extracts numbered capture groups when no named groups");
        String result = RegexExtract.extract(
                "{\"text\":\"order 12345 amount 99.99\"}",
                "text",
                "(\\d+)\\s+\\w+\\s+(\\d+\\.\\d+)",
                null,
                false);
        assertTrue(result.contains("\"group1\":\"12345\""));
        assertTrue(result.contains("\"group2\":\"99.99\""));
    }

    @Test
    public void shouldSkipNonTextualSource() {
        System.out.println("TC: returns input unchanged when source field is not text");
        String input = "{\"count\":42}";
        String result = RegexExtract.extract(input, "count", "(\\d+)", null, false);
        assertEquals(input, result);
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses source, pattern and target config");
        Plugin plugin = new RegexExtract();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"raw\":\"10.0.0.1 GET /health 200\"}"),
                "source=raw;pattern=(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(?<method>\\w+);target=parsed");
        assertEquals(
                "{\"raw\":\"10.0.0.1 GET /health 200\",\"parsed\":{\"ip\":\"10.0.0.1\",\"method\":\"GET\"}}",
                Plugin.toString(result));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnsafeNestedQuantifierRegex() {
        System.out.println("TC: rejects regex patterns with nested quantifiers");
        RegexExtract.extract("{\"msg\":\"aaaaaaaaaaaaaaaa!\"}", "msg", "(a+)+$", null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectOversizedRegexInput() {
        System.out.println("TC: rejects source strings that are too long for regex matching");
        String input = "{\"msg\":\"" + "a".repeat(9000) + "\"}";
        RegexExtract.extract(input, "msg", "a+", null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        RegexExtract.extract("not json", "a", "pattern", null, false);
    }
}
