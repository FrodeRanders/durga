package org.gautelis.durga.plugins;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class KvEnricherTest {

    @Test
    public void shouldEnrichByKey() {
        System.out.println("TC: enriches payload by looking up key field value in inline data map");
        KvEnricher enricher = new KvEnricher("id", Map.of(
                "001", "{\"name\":\"Alice\",\"tier\":\"gold\"}",
                "002", "{\"name\":\"Bob\",\"tier\":\"silver\"}"
        ));
        String result = enricher.enrich("{\"id\":\"001\",\"amount\":100}");
        assertTrue(result.contains("\"name\":\"Alice\""));
        assertTrue(result.contains("\"tier\":\"gold\""));
        assertTrue(result.contains("\"amount\":100"));
        assertTrue(result.contains("\"id\":\"001\""));
    }

    @Test
    public void shouldPassthroughOnMissingKey() {
        System.out.println("TC: passes through unchanged when key field value is not found in inline data");
        KvEnricher enricher = new KvEnricher("id", Map.of("001", "{}"));
        String input = "{\"id\":\"999\",\"amount\":100}";
        assertEquals(input, enricher.enrich(input));
    }

    @Test
    public void shouldPassthroughOnNullKeyField() {
        System.out.println("TC: passes through unchanged when key field is missing from payload");
        KvEnricher enricher = new KvEnricher("id", Map.of());
        String input = "{\"amount\":100}";
        assertEquals(input, enricher.enrich(input));
    }

    @Test
    public void shouldExposeKeyField() {
        System.out.println("TC: exposes configured key field name via getter");
        KvEnricher enricher = new KvEnricher("customerId", Map.of());
        assertEquals("customerId", enricher.keyField());
    }

    @Test
    public void shouldExposeInlineData() {
        System.out.println("TC: exposes inline data map via getter");
        Map<String, String> data = Map.of("k", "{}");
        KvEnricher enricher = new KvEnricher("id", data);
        assertEquals(data, enricher.inlineData());
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses keyField and inline enrichment config");
        Plugin plugin = new KvEnricher();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"id\":\"001\",\"amount\":100}"),
                "keyField=id;inline={001:{\"name\":\"Alice\"}}");
        assertEquals("{\"id\":\"001\",\"amount\":100,\"name\":\"Alice\"}", Plugin.toString(result));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidInputJson() {
        System.out.println("TC: throws IllegalArgumentException when input payload is invalid JSON");
        KvEnricher enricher = new KvEnricher("id", Map.of());
        enricher.enrich("not json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidEnrichmentJson() {
        System.out.println("TC: throws IllegalArgumentException when enrichment data value is invalid JSON");
        KvEnricher enricher = new KvEnricher("id", Map.of("001", "not json"));
        enricher.enrich("{\"id\":\"001\"}");
    }
}
