package org.gautelis.durga.monitoring;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class TimestampsTest {

    @Test
    public void shouldParseValidIso8601() {
        System.out.println("TC: parses a valid ISO-8601 timestamp");
        assertEquals(Instant.parse("2026-04-03T08:00:00Z"),
                Timestamps.parseOrNull("2026-04-03T08:00:00Z"));
    }

    @Test
    public void shouldReturnNullForNullBlankOrMalformed() {
        System.out.println("TC: returns null for null, blank, and malformed timestamps");
        assertNull(Timestamps.parseOrNull(null));
        assertNull(Timestamps.parseOrNull("   "));
        assertNull(Timestamps.parseOrNull("not-a-timestamp"));
        assertNull(Timestamps.parseOrNull("2026-13-99T99:99:99Z"));
    }

    @Test
    public void shouldFallBackForMalformed() {
        System.out.println("TC: parseOrDefault falls back for malformed input and parses valid input");
        Instant fallback = Instant.parse("2000-01-01T00:00:00Z");
        assertEquals(fallback, Timestamps.parseOrDefault("garbage", fallback));
        assertEquals(fallback, Timestamps.parseOrDefault(null, fallback));
        assertEquals(Instant.parse("2026-04-03T08:00:00Z"),
                Timestamps.parseOrDefault("2026-04-03T08:00:00Z", fallback));
    }
}
