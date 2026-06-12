package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class WindowCounterTest {

    @Test
    public void shouldReturnNullUntilWindowCloses() {
        System.out.println("TC: returns null for messages within the same window before flush");
        WindowCounter counter = new WindowCounter(3600, null);
        String result = counter.accept("{\"a\":1}");
        assertNull(result); // first message opens the window
        result = counter.accept("{\"a\":2}");
        assertNull(result); // still same window
    }

    @Test
    public void shouldFlushOnRequest() {
        System.out.println("TC: flush returns summary with totalCount, windowStart and windowEnd");
        WindowCounter counter = new WindowCounter(3600, null);
        counter.accept("{\"a\":1}");
        counter.accept("{\"a\":2}");
        String summary = counter.flush();
        assertTrue(summary.contains("\"totalCount\":2"));
        assertTrue(summary.contains("\"windowStart\""));
        assertTrue(summary.contains("\"windowEnd\""));
    }

    @Test
    public void shouldTrackGroupedCounts() {
        System.out.println("TC: tracks grouped counts by field value and includes groupCounts in summary");
        WindowCounter counter = new WindowCounter(3600, "type");
        counter.accept("{\"type\":\"click\"}");
        counter.accept("{\"type\":\"view\"}");
        counter.accept("{\"type\":\"click\"}");
        String summary = counter.flush();
        assertTrue(summary.contains("\"totalCount\":3"));
        assertTrue(summary.contains("\"groupCounts\""));
        assertTrue(summary.contains("\"click\":2"));
        assertTrue(summary.contains("\"view\":1"));
    }

    @Test
    public void shouldTrackUnparseableAsErrorGroup() {
        System.out.println("TC: counts unparseable JSON as _parse_error_ group");
        WindowCounter counter = new WindowCounter(3600, "type");
        counter.accept("not json");
        String summary = counter.flush();
        assertTrue(summary.contains("\"_parse_error_\":1"));
    }

    @Test
    public void shouldGroupNullFieldAsNull() {
        System.out.println("TC: groups messages with missing groupBy field as _null_");
        WindowCounter counter = new WindowCounter(3600, "type");
        counter.accept("{\"other\":1}");
        String summary = counter.flush();
        assertTrue(summary.contains("\"_null_\":1"));
    }

    @Test
    public void shouldExposeConfig() {
        System.out.println("TC: exposes windowSeconds and groupBy via getters");
        WindowCounter counter = new WindowCounter(120, "group");
        assertEquals(120, counter.windowSeconds());
        assertEquals("group", counter.groupBy());
    }

    @Test
    public void shouldNotIncludeGroupCountsWhenNoGroupBy() {
        System.out.println("TC: does not include groupCounts in summary when no groupBy is configured");
        WindowCounter counter = new WindowCounter(60, null);
        counter.accept("{}");
        String summary = counter.flush();
        assertFalse(summary.contains("groupCounts"));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute via Plugin interface returns null for in-window messages and flush returns summary");
        WindowCounter counter = new WindowCounter();
        String summary = counter.execute("{\"a\":1}", "window=3600");
        assertNull(summary);
        summary = counter.execute("{\"a\":2}", "window=3600");
        assertNull(summary);
        String flushed = counter.flush();
        assertTrue(flushed.contains("\"totalCount\":2"));
    }

    @Test
    public void shouldParseWindowConfigFromExecute() throws Exception {
        System.out.println("TC: parses window and groupBy parameters from config string in execute");
        WindowCounter counter = new WindowCounter();
        String result = counter.execute("{\"type\":\"click\"}", "window=30 groupBy=type");
        assertNull(result);
        String flushed = counter.flush();
        assertTrue(flushed.contains("\"totalCount\":1"));
        assertTrue(flushed.contains("\"click\":1"));
    }
}
