package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class TypeCoercionTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldCoerceToString() throws Exception {
        System.out.println("TC: coerces numeric field to string");
        String result = TypeCoercion.coerce("{\"count\":42}", "count:string");
        JsonNode node = mapper.readTree(result);
        assertTrue(node.get("count").isTextual());
        assertEquals("42", node.get("count").asText());
    }

    @Test
    public void shouldCoerceToInt() throws Exception {
        System.out.println("TC: coerces string field to integer");
        String result = TypeCoercion.coerce("{\"count\":\"42\"}", "count:int");
        JsonNode node = mapper.readTree(result);
        assertTrue(node.get("count").isInt());
        assertEquals(42, node.get("count").asInt());
    }

    @Test
    public void shouldCoerceToDouble() throws Exception {
        System.out.println("TC: coerces string field to double");
        String result = TypeCoercion.coerce("{\"amount\":\"12.5\"}", "amount:double");
        JsonNode node = mapper.readTree(result);
        assertTrue(node.get("amount").isDouble());
        assertEquals(12.5, node.get("amount").asDouble(), 0.001);
    }

    @Test
    public void shouldCoerceToBoolean() throws Exception {
        System.out.println("TC: coerces various truthy values to boolean");
        String result = TypeCoercion.coerce("{\"flag\":\"yes\",\"active\":\"1\"}", "flag:boolean,active:boolean");
        JsonNode node = mapper.readTree(result);
        assertTrue(node.get("flag").asBoolean());
        assertTrue(node.get("active").asBoolean());
    }

    @Test
    public void shouldCoerceToLong() throws Exception {
        System.out.println("TC: coerces string to long");
        String result = TypeCoercion.coerce("{\"timestamp\":\"1700000000000\"}", "timestamp:long");
        JsonNode node = mapper.readTree(result);
        assertTrue(node.get("timestamp").isLong());
        assertEquals(1700000000000L, node.get("timestamp").asLong());
    }

    @Test
    public void shouldPassthroughOnNullConfig() throws Exception {
        System.out.println("TC: returns input unchanged on blank config");
        String input = "{\"a\":1}";
        assertEquals(input, TypeCoercion.coerce(input, ""));
        assertEquals(input, TypeCoercion.coerce(input, null));
    }

    @Test
    public void shouldSkipMissingField() throws Exception {
        System.out.println("TC: skips fields not present in payload");
        String result = TypeCoercion.coerce("{\"a\":1}", "b:int");
        assertTrue(result.contains("\"a\":1"));
    }

    @Test
    public void shouldHandleNestedField() throws Exception {
        System.out.println("TC: coerces nested field via dot-notation");
        String result = TypeCoercion.coerce("{\"data\":{\"count\":\"99\"}}", "data.count:int");
        JsonNode node = mapper.readTree(result);
        assertEquals(99, node.get("data").get("count").asInt());
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute applies type coercion expression");
        Plugin plugin = new TypeCoercion();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"amount\":\"12.5\",\"order_id\":\"7\"}"),
                "amount:double, order_id:int");
        JsonNode node = mapper.readTree(Plugin.toString(result));
        assertTrue(node.get("amount").isDouble());
        assertEquals(7, node.get("order_id").asInt());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        System.out.println("TC: throws IllegalArgumentException on malformed input");
        TypeCoercion.coerce("not json", "a:int");
    }
}
