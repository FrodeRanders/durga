package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class WindowCounterTest {

    @Test
    public void shouldReturnNullUntilWindowCloses() {
        WindowCounter counter = new WindowCounter(3600, null);
        String result = counter.accept("{\"a\":1}");
        assertNull(result); // first message opens the window
        result = counter.accept("{\"a\":2}");
        assertNull(result); // still same window
    }

    @Test
    public void shouldFlushOnRequest() {
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
        WindowCounter counter = new WindowCounter(3600, "type");
        counter.accept("not json");
        String summary = counter.flush();
        assertTrue(summary.contains("\"_parse_error_\":1"));
    }

    @Test
    public void shouldGroupNullFieldAsNull() {
        WindowCounter counter = new WindowCounter(3600, "type");
        counter.accept("{\"other\":1}");
        String summary = counter.flush();
        assertTrue(summary.contains("\"_null_\":1"));
    }

    @Test
    public void shouldExposeConfig() {
        WindowCounter counter = new WindowCounter(120, "group");
        assertEquals(120, counter.windowSeconds());
        assertEquals("group", counter.groupBy());
    }

    @Test
    public void shouldNotIncludeGroupCountsWhenNoGroupBy() {
        WindowCounter counter = new WindowCounter(60, null);
        counter.accept("{}");
        String summary = counter.flush();
        assertFalse(summary.contains("groupCounts"));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
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
        WindowCounter counter = new WindowCounter();
        String result = counter.execute("{\"type\":\"click\"}", "window=30 groupBy=type");
        assertNull(result);
        String flushed = counter.flush();
        assertTrue(flushed.contains("\"totalCount\":1"));
        assertTrue(flushed.contains("\"click\":1"));
    }
}
