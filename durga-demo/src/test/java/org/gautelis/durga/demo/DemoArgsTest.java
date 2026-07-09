package org.gautelis.durga.demo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DemoArgsTest {

    @Test
    public void parseLongReturnsParsedValue() {
        System.out.println("TC: DemoArgs parses a numeric long value");
        assertEquals(250L, DemoArgs.parseLong("250", 100L, "interval"));
    }

    @Test
    public void parseLongReturnsFallbackForBlankOrInvalidValues() {
        System.out.println("TC: DemoArgs returns fallback for blank or invalid long values");
        assertEquals(100L, DemoArgs.parseLong("", 100L, "interval"));
        assertEquals(100L, DemoArgs.parseLong("not-a-number", 100L, "interval"));
    }
}
