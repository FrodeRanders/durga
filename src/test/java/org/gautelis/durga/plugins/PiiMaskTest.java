package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class PiiMaskTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldMaskDefaultChar() throws Exception {
        System.out.println("TC: masks field with default '*' character");
        String result = PiiMask.mask("{\"ssn\":\"123-45-6789\"}", "ssn", '*', 0);
        assertEquals("{\"ssn\":\"***********\"}", result);
    }

    @Test
    public void shouldPreserveBoundaryChars() throws Exception {
        System.out.println("TC: preserves specified number of characters at boundaries");
        String result = PiiMask.mask("{\"email\":\"alice@example.com\"}", "email", '*', 3);
        assertEquals("{\"email\":\"ali***********com\"}", result);
    }

    @Test
    public void shouldPreserveFourChars() throws Exception {
        System.out.println("TC: preserves 4 chars at start and end");
        String result = PiiMask.mask("{\"card\":\"4111111111111111\"}", "card", '#', 4);
        assertEquals("{\"card\":\"4111########1111\"}", result);
    }

    @Test
    public void shouldMaskMultipleFields() throws Exception {
        System.out.println("TC: masks multiple specified fields");
        String result = PiiMask.mask(
                "{\"name\":\"John\",\"ssn\":\"111-22-3333\",\"phone\":\"555-1234\"}",
                "ssn,phone", '*', 0);
        assertTrue(result.contains("\"ssn\":\"***********\""));
        assertTrue(result.contains("\"phone\":\"********\""));
        assertTrue(result.contains("\"name\":\"John\""));
    }

    @Test
    public void shouldMaskNestedField() throws Exception {
        System.out.println("TC: masks nested field via dot-notation");
        String result = PiiMask.mask(
                "{\"user\":{\"ssn\":\"999-88-7777\",\"name\":\"Jane\"}}",
                "user.ssn", '*', 0);
        assertTrue(result.contains("\"ssn\":\"***********\""));
        assertTrue(result.contains("\"name\":\"Jane\""));
    }

    @Test
    public void shouldPassthroughOnNullFields() {
        System.out.println("TC: returns input unchanged when fields config is blank");
        String input = "{\"a\":1}";
        assertEquals(input, PiiMask.mask(input, "", '*', 0));
        assertEquals(input, PiiMask.mask(input, null, '*', 0));
    }

    @Test
    public void shouldPassthroughOnShortText() {
        System.out.println("TC: fully masks text shorter than preserve window");
        String result = PiiMask.mask("{\"x\":\"ab\"}", "x", '*', 5);
        assertEquals("{\"x\":\"**\"}", result);
    }

    @Test
    public void shouldHandleEmptyString() {
        System.out.println("TC: returns empty string unchanged");
        String result = PiiMask.mask("{\"x\":\"\"}", "x", '*', 0);
        assertEquals("{\"x\":\"\"}", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        PiiMask.mask("not json", "a", '*', 0);
    }
}
