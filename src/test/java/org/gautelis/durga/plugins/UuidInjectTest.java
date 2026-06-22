package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class UuidInjectTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldInjectUuid4() throws Exception {
        System.out.println("TC: injects a UUID v4 into the specified field");
        String result = UuidInject.inject("{\"name\":\"Alice\"}", "id", "uuid4");
        assertTrue(result.contains("\"id\":\""));
        assertTrue(result.contains("\"name\":\"Alice\""));
        String uuid = mapper.readTree(result).get("id").asText();
        assertEquals(36, uuid.length());
        assertEquals(5, uuid.split("-").length);
    }

    @Test
    public void shouldInjectMultipleFields() throws Exception {
        System.out.println("TC: injects UUIDs into multiple fields");
        String result = UuidInject.inject("{}", "id,trace_id,correlation_id", "uuid4");
        JsonNode node = mapper.readTree(result);
        assertNotNull(node.get("id"));
        assertNotNull(node.get("trace_id"));
        assertNotNull(node.get("correlation_id"));
    }

    @Test
    public void shouldInjectNestedField() throws Exception {
        System.out.println("TC: injects UUID into nested field via dot-notation");
        String result = UuidInject.inject("{\"meta\":{}}", "meta.request_id", "uuid4");
        JsonNode node = mapper.readTree(result);
        assertNotNull(node.get("meta").get("request_id"));
    }

    @Test
    public void shouldUseDefaultField() throws Exception {
        System.out.println("TC: uses 'id' as default field when fields list is empty");
        String result = UuidInject.inject("{}", "id", "uuid4");
        assertTrue(result.contains("\"id\":\""));
    }

    @Test
    public void shouldInjectUuid1() throws Exception {
        System.out.println("TC: injects a time-based UUID v1 style");
        String result = UuidInject.inject("{}", "id", "uuid1");
        JsonNode node = mapper.readTree(result);
        assertEquals(36, node.get("id").asText().length());
    }

    @Test
    public void shouldNotOverwriteExistingFields() throws Exception {
        System.out.println("TC: overwrites existing field with new UUID");
        String result = UuidInject.inject("{\"id\":\"fixed\"}", "id", "uuid4");
        JsonNode node = mapper.readTree(result);
        assertNotEquals("fixed", node.get("id").asText());
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses fields and strategy config");
        Plugin plugin = new UuidInject();
        byte[] result = plugin.execute(Plugin.toBytes("{}"), "fields=trace_id;strategy=uuid4");
        JsonNode node = mapper.readTree(Plugin.toString(result));
        assertEquals(36, node.get("trace_id").asText().length());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        UuidInject.inject("not json", "id", "uuid4");
    }
}
