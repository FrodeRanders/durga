package org.gautelis.durga;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ProcessStateTest {

    @Test
    public void shouldRoundTripThroughJson() {
        List<ProcessState.Token> tokens = List.of(
                new ProcessState.Token("token-1", "activity-A"),
                new ProcessState.Token("token-2", "activity-B")
        );
        ProcessState state = new ProcessState(
                "pi-1",
                tokens,
                Map.of("var1", "value1", "var2", 42),
                7L
        );

        String json = state.toJson();
        ProcessState parsed = ProcessState.fromJson(json);

        assertEquals("pi-1", parsed.processInstanceId());
        assertEquals(7L, parsed.version());
        assertEquals(2, parsed.tokens().size());
        assertEquals("token-1", parsed.tokens().get(0).tokenId());
        assertEquals("activity-A", parsed.tokens().get(0).at());
        assertEquals("token-2", parsed.tokens().get(1).tokenId());
        assertEquals("activity-B", parsed.tokens().get(1).at());
        assertEquals("value1", parsed.variables().get("var1"));
        assertEquals(42, parsed.variables().get("var2"));
    }

    @Test
    public void shouldHandleEmptyCollections() {
        ProcessState state = new ProcessState("pi-2", List.of(), Map.of(), 0L);

        String json = state.toJson();
        ProcessState parsed = ProcessState.fromJson(json);

        assertEquals("pi-2", parsed.processInstanceId());
        assertTrue(parsed.tokens().isEmpty());
        assertTrue(parsed.variables().isEmpty());
        assertEquals(0L, parsed.version());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidJson() {
        ProcessState.fromJson("{invalid");
    }
}
