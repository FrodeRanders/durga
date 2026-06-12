package org.gautelis.durga.plugins;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class JsonTransformTest {

    @Test
    public void shouldRemapSimpleFields() {
        String input = "{\"name\":\"Alice\",\"age\":30,\"city\":\"Oslo\"}";
        String result = JsonTransform.transform(input, "name, age");
        assertTrue(result.contains("\"name\":\"Alice\""));
        assertTrue(result.contains("\"age\":30"));
        assertFalse(result.contains("\"city\""));
    }

    @Test
    public void shouldMapSourceToDest() {
        String input = "{\"data\":{\"name\":\"Bob\",\"amount\":100}}";
        String result = JsonTransform.transform(input, "data.name:customerName, data.amount:total");
        assertTrue(result.contains("\"customerName\":\"Bob\""));
        assertTrue(result.contains("\"total\":100"));
        assertFalse(result.contains("\"data\""));
    }

    @Test
    public void shouldHandleLiteralValues() {
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "a, status:active, count:42, flag:true");
        assertTrue(result.contains("\"a\":1"));
        assertTrue(result.contains("\"status\":\"active\""));
        assertTrue(result.contains("\"count\":42"));
        assertTrue(result.contains("\"flag\":true"));
    }

    @Test
    public void shouldPassthroughOnIdentity() {
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, ".");
        assertEquals(input, result);
    }

    @Test
    public void shouldPassthroughOnBlankExpression() {
        String input = "{\"a\":1}";
        assertEquals(input, JsonTransform.transform(input, ""));
        assertEquals(input, JsonTransform.transform(input, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        JsonTransform.transform("not json", "a");
    }

    @Test
    public void shouldSkipNullSourceFields() {
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "a, missing");
        assertTrue(result.contains("\"a\":1"));
        assertFalse(result.contains("missing"));
    }

    @Test
    public void shouldSetLiteralWhenSourceNotFound() {
        String input = "{\"a\":1}";
        String result = JsonTransform.transform(input, "b:hello, count:42");
        assertTrue(result.contains("\"b\":\"hello\""));
        assertTrue(result.contains("\"count\":42"));
    }

    @Test
    public void shouldMapExistingFieldToNewPath() {
        String input = "{\"data\":{\"x\":1}}";
        String result = JsonTransform.transform(input, "data.x:topLevel");
        assertTrue(result.contains("\"topLevel\":1"));
        assertFalse(result.contains("\"data\""));
    }
}
