package org.gautelis.durga.plugins;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class KvEnricherTest {

    @Test
    public void shouldEnrichByKey() {
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
        KvEnricher enricher = new KvEnricher("id", Map.of("001", "{}"));
        String input = "{\"id\":\"999\",\"amount\":100}";
        assertEquals(input, enricher.enrich(input));
    }

    @Test
    public void shouldPassthroughOnNullKeyField() {
        KvEnricher enricher = new KvEnricher("id", Map.of());
        String input = "{\"amount\":100}";
        assertEquals(input, enricher.enrich(input));
    }

    @Test
    public void shouldExposeKeyField() {
        KvEnricher enricher = new KvEnricher("customerId", Map.of());
        assertEquals("customerId", enricher.keyField());
    }

    @Test
    public void shouldExposeInlineData() {
        Map<String, String> data = Map.of("k", "{}");
        KvEnricher enricher = new KvEnricher("id", data);
        assertEquals(data, enricher.inlineData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidInputJson() {
        KvEnricher enricher = new KvEnricher("id", Map.of());
        enricher.enrich("not json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidEnrichmentJson() {
        KvEnricher enricher = new KvEnricher("id", Map.of("001", "not json"));
        enricher.enrich("{\"id\":\"001\"}");
    }
}
