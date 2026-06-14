package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class JsonTransformTest {

    @Test
    public void shouldRemapSimpleFields() {
        System.out.println("TC: transforms fields by copying specified fields and dropping others");
        String input = "{\"name\":\"Alice\",\"age\":30,\"city\":\"Oslo\"}";
        String result = JsonTransform.transform(input, "name, age");
        assertTrue(result.contains("\"name\":\"Alice\""));
        assertTrue(result.contains("\"age\":30"));
        assertFalse(result.contains("\"city\""));
    }

    @Test
    public void shouldMapSourceToDest() {
        System.out.println("TC: maps nested source fields to top-level destination fields");
        String input = "{\"data\":{\"name\":\"Bob\",\"amount\":100}}";
        String result = JsonTransform.transform(input, "data.name:customerName, data.amount:total");
        assertTrue(result.contains("\"customerName\":\"Bob\""));
        assertTrue(result.contains("\"total\":100"));
        assertFalse(result.contains("\"data\""));
    }

    @Test
    public void shouldHandleLiteralValues() {
        System.out.println("TC: inserts literal string, numeric and boolean values into output");
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "a, status:active, count:42, flag:true");
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"status\":\"active\""));
        assertTrue(result.contains("\"count\":42"));
        assertTrue(result.contains("\"flag\":true"));
    }

    @Test
    public void shouldPassthroughOnIdentity() {
        System.out.println("TC: passes through input unchanged when expression is dot identity");
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, ".");
        assertEquals(input, result);
    }

    @Test
    public void shouldPassthroughOnBlankExpression() {
        System.out.println("TC: passes through input unchanged on empty or null expression");
        String input = "{\"a\":1}";
        assertEquals(input, JsonTransform.transform(input, ""));
        assertEquals(input, JsonTransform.transform(input, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws IllegalArgumentException on invalid JSON input");
        JsonTransform.transform("not json", "a");
    }

    @Test
    public void shouldSkipNullSourceFields() {
        System.out.println("TC: skips source fields that are missing in input without failing");
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "a, missing");
        assertTrue(result.contains("\"a\":1"));
        assertFalse(result.contains("missing"));
    }

    @Test
    public void shouldSetLiteralWhenSourceNotFound() {
        System.out.println("TC: sets literal value even when source field is not found in input");
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "b:hello, count:42");
        assertTrue(result.contains("\"b\":\"hello\""));
        assertTrue(result.contains("\"count\":42"));
    }

    @Test
    public void shouldMapExistingFieldToNewPath() {
        System.out.println("TC: maps an existing nested field to a new top-level path and drops source");
        String input = "{\"data\":{\"x\":1}}";
        String result = JsonTransform.transform(input, "data.x:topLevel");
        assertTrue(result.contains("\"topLevel\":1"));
        assertFalse(result.contains("\"data\""));
    }
}
