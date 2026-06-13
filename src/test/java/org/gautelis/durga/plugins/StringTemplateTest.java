package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class StringTemplateTest {

    @Test
    public void shouldInterpolateFields() {
        System.out.println("TC: replaces template tokens with payload field values");
        String result = StringTemplate.render(
                "{\"name\":\"Alice\",\"amount\":100,\"city\":\"Oslo\"}",
                "Hello ${name}, your order of ${amount} ships to ${city}");
        assertEquals("Hello Alice, your order of 100 ships to Oslo", result);
    }

    @Test
    public void shouldHandleMissingFields() {
        System.out.println("TC: replaces missing fields with empty string");
        String result = StringTemplate.render(
                "{\"name\":\"Bob\"}",
                "Hi ${name}! Your ${missing} is here");
        assertEquals("Hi Bob! Your  is here", result);
    }

    @Test
    public void shouldAccessNestedFields() {
        System.out.println("TC: resolves dot-notation nested fields in templates");
        String result = StringTemplate.render(
                "{\"customer\":{\"name\":\"Charlie\",\"tier\":\"gold\"}}",
                "${customer.name} is ${customer.tier} tier");
        assertEquals("Charlie is gold tier", result);
    }

    @Test
    public void shouldReturnInputOnNullTemplate() {
        System.out.println("TC: returns payload unchanged when template is null/blank");
        String input = "{\"a\":1}";
        String result = StringTemplate.render(input, null);
        assertEquals(input, result);
    }

    @Test
    public void shouldHandleNoTokens() {
        System.out.println("TC: returns literal template when no tokens present");
        String result = StringTemplate.render("{\"a\":1}", "static text");
        assertEquals("static text", result);
    }

    @Test
    public void shouldHandleNumericValues() {
        System.out.println("TC: formats numeric values as text in template");
        String result = StringTemplate.render(
                "{\"total\":150.75}",
                "Amount: ${total}");
        assertEquals("Amount: 150.75", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        StringTemplate.render("not json", "${field}");
    }
}
