package org.gautelis.durga.plugins;

import java.nio.charset.StandardCharsets;

/**
 * Contract for data pipeline plugins.
 * <p>
 * Plugins receive a raw payload and a configuration string, and return a
 * transformed payload. If the transformation fails, they throw.
 * <p>
 * The payload is {@code byte[]} to avoid assumptions about character
 * encoding. Convenience methods {@link #toString(byte[])} and
 * {@link #toBytes(String)} handle the common case of UTF-8 text data.
 * <p>
 * The config is a plain {@code String} — it originates from Camunda
 * extension properties in the BPMN model and is always text-based.
 */
public interface Plugin {

    /**
     * Execute the plugin against a raw payload.
     *
     * @param payload raw input bytes
     * @param config  plugin configuration string (format varies by plugin)
     * @return raw output bytes
     * @throws Exception if processing fails
     */
    byte[] execute(byte[] payload, String config) throws Exception;

    static String toString(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }

    static byte[] toBytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }
}
