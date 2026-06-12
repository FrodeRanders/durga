package org.gautelis.durga.plugins;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class DeadLetterRouterTest {

    @Test
    public void shouldRouteOnFieldValue() {
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of(
                "ok", "success",
                "error", "failure"
        ), "unknown");
        assertEquals("success", router.route("{\"status\":\"ok\",\"data\":{}}"));
        assertEquals("failure", router.route("{\"status\":\"error\",\"msg\":\"boom\"}"));
    }

    @Test
    public void shouldFallThroughToDefault() {
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("{\"status\":\"pending\"}"));
    }

    @Test
    public void shouldFallThroughOnMissingField() {
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("{\"other\":1}"));
    }

    @Test
    public void shouldFallThroughOnInvalidJson() {
        DeadLetterRouter router = new DeadLetterRouter("status", Map.of("ok", "success"), "default");
        assertEquals("default", router.route("not json"));
    }

    @Test
    public void shouldExposeConfig() {
        var routes = Map.of("a", "b");
        DeadLetterRouter router = new DeadLetterRouter("field", routes, "fallback");
        assertEquals("field", router.field());
        assertEquals(routes, router.routes());
        assertEquals("fallback", router.defaultRoute());
    }

    @Test
    public void shouldRouteNestedField() {
        DeadLetterRouter router = new DeadLetterRouter("event.type", Map.of("click", "clicks_topic"), "other");
        assertEquals("clicks_topic", router.route("{\"event\":{\"type\":\"click\"}}"));
    }
}
