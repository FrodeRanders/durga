package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class PluginResultTest {

    @Test
    public void shouldDefaultToPayloadDisposition() {
        System.out.println("TC: success results default to PAYLOAD disposition");
        PluginResult result = PluginResult.success(Plugin.toBytes("{}"), "k");
        assertEquals(PluginResult.OutputDisposition.PAYLOAD, result.disposition());
        assertTrue(result.isSuccess());
    }

    @Test
    public void shouldCarryPassthroughDisposition() {
        System.out.println("TC: passthrough factory marks output as a non-replacing control value");
        PluginResult result = PluginResult.passthrough(Plugin.toBytes("high"), "k");
        assertEquals(PluginResult.OutputDisposition.PASSTHROUGH, result.disposition());
        assertTrue(result.isSuccess());
    }

    @Test
    public void shouldCarrySideEffectDisposition() {
        System.out.println("TC: sideEffect factory marks output as an external side-effect handle");
        PluginResult result = PluginResult.sideEffect(Plugin.toBytes("{\"uri\":\"s3://x\"}"), "k", "stored");
        assertEquals(PluginResult.OutputDisposition.SIDE_EFFECT, result.disposition());
        assertEquals("stored", result.sideEffectDescription());
    }

    @Test
    public void shouldKeepPayloadDispositionForControlStrategies() {
        System.out.println("TC: skip/fail/dlq results retain PAYLOAD disposition");
        assertEquals(PluginResult.OutputDisposition.PAYLOAD, PluginResult.skip("k", "r").disposition());
        assertEquals(PluginResult.OutputDisposition.PAYLOAD, PluginResult.fail("k", "r").disposition());
        assertEquals(PluginResult.OutputDisposition.PAYLOAD,
                PluginResult.dlq(Plugin.toBytes("{}"), "k", "r").disposition());
    }
}
