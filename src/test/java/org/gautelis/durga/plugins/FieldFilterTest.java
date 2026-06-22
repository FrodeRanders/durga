package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldFilterTest {

    @Test
    public void shouldKeepWhitelistedFields() {
        System.out.println("TC: keeps only whitelisted fields and drops all others");
        String input = "{\"a\":1,\"b\":2,\"c\":3}";
        String result = FieldFilter.filter(input, "a,c", null, null);
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"c\":3"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldDropBlacklistedFields() {
        System.out.println("TC: drops blacklisted fields and keeps all others");
        String input = "{\"a\":1,\"b\":2,\"c\":3}";
        String result = FieldFilter.filter(input, null, "b", null);
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"c\":3"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldKeepWinsOnConflict() {
        System.out.println("TC: whitelist takes precedence over blacklist on field conflict");
        String input = "{\"a\":1,\"b\":2}";
        String result = FieldFilter.filter(input, "a", "a,b", null);
        assertTrue(result.contains("\"a\":1"));
        assertFalse(result.contains("\"b\":"));
    }

    @Test
    public void shouldPassthroughWhenNoFilters() {
        System.out.println("TC: passes through input unchanged when no whitelist or blacklist is set");
        String input = "{\"a\":1,\"b\":2}";
        assertEquals(input, FieldFilter.filter(input, null, null, null));
        assertEquals(input, FieldFilter.filter(input, "", "", null));
    }

    @Test
    public void shouldPassthroughNonObject() {
        System.out.println("TC: passes through non-object JSON values unchanged");
        String input = "\"hello\"";
        assertEquals(input, FieldFilter.filter(input, "a", null, null));
    }

    @Test
    public void shouldFlattenNestedPrefix() {
        System.out.println("TC: flattens nested object fields to top level and drops the nesting key");
        String input = "{\"a\":1,\"data\":{\"x\":10,\"y\":20}}";
        String result = FieldFilter.filter(input, null, "data", "data");
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"x\":10"));
        assertTrue(result.contains("\"y\":20"));
        assertFalse(result.contains("\"data\""));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses keep/drop config and filters payload");
        Plugin plugin = new FieldFilter();
        byte[] result = plugin.execute(Plugin.toBytes("{\"a\":1,\"b\":2,\"c\":3}"), "keep=a,c drop=b");
        assertEquals("{\"a\":1,\"c\":3}", Plugin.toString(result));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws IllegalArgumentException on invalid JSON input");
        FieldFilter.filter("not json", "a", null, null);
    }
}
