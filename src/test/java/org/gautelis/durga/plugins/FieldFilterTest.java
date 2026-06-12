package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldFilterTest {

    @Test
    public void shouldKeepWhitelistedFields() {
        String input = "{\"a\":1,\"b\":2,\"c\":3}";
        String result = FieldFilter.filter(input, "a,c", null, null);
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"c\":3"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldDropBlacklistedFields() {
        String input = "{\"a\":1,\"b\":2,\"c\":3}";
        String result = FieldFilter.filter(input, null, "b", null);
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"c\":3"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldKeepWinsOnConflict() {
        String input = "{\"a\":1,\"b\":2}";
        String result = FieldFilter.filter(input, "a", "a,b", null);
        assertTrue(result.contains("\"a\":1"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldPassthroughWhenNoFilters() {
        String input = "{\"a\":1,\"b\":2}";
        assertEquals(input, FieldFilter.filter(input, null, null, null));
        assertEquals(input, FieldFilter.filter(input, "", "", null));
    }

    @Test
    public void shouldPassthroughNonObject() {
        String input = "\"hello\"";
        assertEquals(input, FieldFilter.filter(input, "a", null, null));
    }

    @Test
    public void shouldFlattenNestedPrefix() {
        String input = "{\"a\":1,\"data\":{\"x\":10,\"y\":20}}";
        String result = FieldFilter.filter(input, null, "data", "data");
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"x\":10"));
        assertTrue(result.contains("\"y\":20"));
        assertFalse(result.contains("\"data\""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        FieldFilter.filter("not json", "a", null, null);
    }
}
