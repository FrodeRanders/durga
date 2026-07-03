package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonFlattenTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldFlattenNestedObject() throws Exception {
        System.out.println("TC: flattens nested object to dot-notation keys");
        String result = JsonFlatten.flatten(
                "{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Oslo\"}}}", ".", Integer.MAX_VALUE);
        assertTrue(result.contains("\"user.name\":\"Alice\""));
        assertTrue(result.contains("\"user.address.city\":\"Oslo\""));
    }

    @Test
    public void shouldRespectMaxDepth() throws Exception {
        System.out.println("TC: respects max depth when flattening");
        String result = JsonFlatten.flatten(
                "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}", ".", 2);
        assertTrue(result.contains("\"a.b.c\""));
        assertFalse(result.contains("\"a.b.c.d\""));
    }

    @Test
    public void shouldUnflattenDotNotation() throws Exception {
        System.out.println("TC: unflattens dot-notation keys to nested object");
        String result = JsonFlatten.unflatten(
                "{\"a.b\":1,\"a.c\":2,\"x.y.z\":3}", ".");
        JsonNode node = mapper.readTree(result);
        assertEquals(1, node.get("a").get("b").asInt());
        assertEquals(2, node.get("a").get("c").asInt());
        assertEquals(3, node.get("x").get("y").get("z").asInt());
    }

    @Test
    public void shouldRoundtrip() throws Exception {
        System.out.println("TC: flatten + unflatten produces original structure");
        String original = "{\"user\":{\"name\":\"Bob\",\"age\":30}}";
        String flat = JsonFlatten.flatten(original, ".", Integer.MAX_VALUE);
        String nested = JsonFlatten.unflatten(flat, ".");
        assertEquals(mapper.readTree(original), mapper.readTree(nested));
    }

    @Test
    public void shouldPassthroughNonObject() {
        System.out.println("TC: returns primitive/array inputs unchanged");
        String input = "[1,2,3]";
        assertEquals(input, JsonFlatten.flatten(input, ".", Integer.MAX_VALUE));
        assertEquals(input, JsonFlatten.unflatten(input, "."));
    }

    @Test
    public void shouldUseCustomSeparator() throws Exception {
        System.out.println("TC: uses custom separator for keys");
        String result = JsonFlatten.flatten(
                "{\"a\":{\"b\":1}}", "__", Integer.MAX_VALUE);
        assertTrue(result.contains("\"a__b\":1"));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses direction, separator and maxDepth config");
        Plugin plugin = new JsonFlatten();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"a\":{\"b\":{\"c\":1}}}"),
                "direction=flatten;separator=.;maxDepth=1");
        assertEquals("{\"a.b\":{\"c\":1}}", Plugin.toString(result));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws on malformed input JSON");
        JsonFlatten.flatten("not json", ".", Integer.MAX_VALUE);
    }
}
