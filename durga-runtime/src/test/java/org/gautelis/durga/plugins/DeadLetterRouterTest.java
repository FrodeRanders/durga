package org.gautelis.durga.plugins;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class DeadLetterRouterTest {

    @Test
    public void shouldRouteOnFieldValue() {
        System.out.println("TC: routes to correct topic based on field value matching routes map");
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of(
                "ok", "success",
                "error", "failure"
        ), "unknown");
        assertEquals("success", router.route("{\"status\":\"ok\",\"data\":{}}"));
        assertEquals("failure", router.route("{\"status\":\"error\",\"msg\":\"boom\"}"));
    }

    @Test
    public void shouldFallThroughToDefault() {
        System.out.println("TC: returns default route when field value does not match any route");
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("{\"status\":\"pending\"}"));
    }

    @Test
    public void shouldFallThroughOnMissingField() {
        System.out.println("TC: returns default route when routing field is missing from payload");
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("{\"other\":1}"));
    }

    @Test
    public void shouldFallThroughOnInvalidJson() {
        System.out.println("TC: returns default route when payload is invalid JSON");
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("not json"));
    }

    @Test
    public void shouldExposeConfig() {
        System.out.println("TC: exposes field, routes map and default route via getters");
        var routes = Map.of("a", "b");
        DeadLetterRouter router = new DeadLetterRouter("field", routes, "fallback");
        assertEquals("field", router.field());
        assertEquals(routes, router.routes());
        assertEquals("fallback", router.defaultRoute());
    }

    @Test
    public void shouldRouteNestedField() {
        System.out.println("TC: routes on nested field value using dot-separated path");
        DeadLetterRouter router = new DeadLetterRouter("event.type", Map.of("click", "clicks_topic"), "other");
        assertEquals("clicks_topic", router.route("{\"event\":{\"type\":\"click\"}}"));
    }

    @Test
    public void shouldExecuteViaPluginInterface() throws Exception {
        System.out.println("TC: execute parses field, routes and default route config");
        Plugin plugin = new DeadLetterRouter();
        byte[] result = plugin.execute(
                Plugin.toBytes("{\"status\":\"error\"}"),
                "field=status routes={ok:success,error:failure} default=other");
        assertEquals("failure", Plugin.toString(result));
    }
}
