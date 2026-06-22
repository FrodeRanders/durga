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
        System.out.println("TC: fieldAt retrieves value at dot-separated nested path");
        JsonNode node = mapper.readTree("{\"a\":{\"b\":{\"c\":42}}}");
        JsonNode result = PipelinePlugin.fieldAt(node, "a.b.c");
        assertEquals(42, result.asInt());
    }

    @Test
    public void shouldReturnNullOnMissingPath() throws Exception {
        System.out.println("TC: fieldAt returns null when intermediate path segment is missing");
        JsonNode node = mapper.readTree("{\"a\":1}");
        assertNull(PipelinePlugin.fieldAt(node, "a.b.c"));
    }

    @Test
    public void shouldSetNestedField() throws Exception {
        System.out.println("TC: setFieldAt creates intermediate objects and sets value at nested path");
        var node = mapper.createObjectNode();
        PipelinePlugin.setFieldAt(node, "a.b.c", mapper.getNodeFactory().numberNode(99));
        assertEquals(99, node.get("a").get("b").get("c").asInt());
    }

    @Test
    public void shouldShallowMerge() throws Exception {
        System.out.println("TC: shallowMerge overwrites matching keys and adds new keys from override");
        JsonNode base = mapper.readTree("{\"a\":1,\"b\":2}");
        JsonNode override = mapper.readTree("{\"b\":99,\"c\":3}");
        JsonNode merged = PipelinePlugin.shallowMerge(base, override);
        assertEquals(1, merged.get("a").asInt());
        assertEquals(99, merged.get("b").asInt());
        assertEquals(3, merged.get("c").asInt());
    }

    @Test
    public void shouldCreateErrorRecord() {
        System.out.println("TC: errorRecord creates JSON error record without embedding original payload");
        String record = PipelinePlugin.errorRecord("{\"secret\":\"value\"}", "test", "something broke");
        assertTrue(record.contains("\"plugin\":\"test\""));
        assertTrue(record.contains("\"error\":\"something broke\""));
        assertTrue(record.contains("\"originalBytes\""));
        assertTrue(record.contains("\"originalSha256\""));
        assertFalse(record.contains("secret"));
        assertFalse(record.contains("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectTooDeepPath() {
        System.out.println("TC: setFieldAt rejects paths with too many segments");
        var node = mapper.createObjectNode();
        PipelinePlugin.setFieldAt(node,
                "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z.aa.ab.ac.ad.ae.af.ag",
                mapper.getNodeFactory().numberNode(1));
    }

    @Test
    public void shouldConvertMapToJson() {
        System.out.println("TC: mapToJson converts a Map of mixed types to JSON string");
        String json = PipelinePlugin.mapToJson(Map.of("a", "hello", "b", 42, "c", true));
        assertTrue(json.contains("\"a\":\"hello\""));
        assertTrue(json.contains("\"b\":42"));
        assertTrue(json.contains("\"c\":true"));
    }

    @Test
    public void shouldHandleNonObjectFieldAt() throws Exception {
        System.out.println("TC: fieldAt returns null when node is not an object");
        JsonNode node = mapper.readTree("42");
        assertNull(PipelinePlugin.fieldAt(node, "anything"));
    }
}
