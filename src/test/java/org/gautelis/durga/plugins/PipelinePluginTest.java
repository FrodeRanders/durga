package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class PipelinePluginTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldAccessNestedField() throws Exception {
        JsonNode node = mapper.readTree("{\"a\":{\"b\":{\"c\":42}}}");
        JsonNode result = PipelinePlugin.fieldAt(node, "a.b.c");
        assertEquals(42, result.asInt());
    }

    @Test
    public void shouldReturnNullOnMissingPath() throws Exception {
        JsonNode node = mapper.readTree("{\"a\":1}");
        assertNull(PipelinePlugin.fieldAt(node, "a.b.c"));
    }

    @Test
    public void shouldSetNestedField() throws Exception {
        var node = mapper.createObjectNode();
        PipelinePlugin.setFieldAt(node, "a.b.c", mapper.getNodeFactory().numberNode(99));
        assertEquals(99, node.get("a").get("b").get("c").asInt());
    }

    @Test
    public void shouldShallowMerge() throws Exception {
        JsonNode base = mapper.readTree("{\"a\":1,\"b\":2}");
        JsonNode override = mapper.readTree("{\"b\":99,\"c\":3}");
        JsonNode merged = PipelinePlugin.shallowMerge(base, override);
        assertEquals(1, merged.get("a").asInt());
        assertEquals(99, merged.get("b").asInt());
        assertEquals(3, merged.get("c").asInt());
    }

    @Test
    public void shouldCreateErrorRecord() {
        String record = PipelinePlugin.errorRecord("{\"a\":1}", "test", "something broke");
        assertTrue(record.contains("\"plugin\":\"test\""));
        assertTrue(record.contains("\"error\":\"something broke\""));
        assertTrue(record.contains("\"original\""));
    }

    @Test
    public void shouldConvertMapToJson() {
        String json = PipelinePlugin.mapToJson(Map.of("a", "hello", "b", 42, "c", true));
        assertTrue(json.contains("\"a\":\"hello\""));
        assertTrue(json.contains("\"b\":42"));
        assertTrue(json.contains("\"c\":true"));
    }

    @Test
    public void shouldHandleNonObjectFieldAt() throws Exception {
        JsonNode node = mapper.readTree("42");
        assertNull(PipelinePlugin.fieldAt(node, "anything"));
    }
}
