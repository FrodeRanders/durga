package org.gautelis.durga.plugins;

import java.nio.charset.StandardCharsets;

/**
 * Contract for data pipeline plugins.
 * <p>
 * Plugins receive a raw payload and a configuration string, and return a
 * transformed payload. If the transformation fails, they throw.
 * <p>
 * The generated worker always calls {@link #execute(byte[], String)}.
 * <ul>
 * <li><b>Text / JSON plugins</b> — override {@link #execute(String, String)}.
 *     The default {@code byte[]} overload handles UTF-8 conversion.</li>
 * <li><b>Binary plugins</b> — override {@link #execute(byte[], String)} directly.</li>
 * </ul>
 */
public interface Plugin {

    /**
     * Called by the generated worker. Override for binary payloads.
     * <p>
     * Default converts payload to a UTF-8 String and delegates to
     * {@link #execute(String, String)}, then converts the result back.
     *
     * @param payload raw input bytes
     * @param config  plugin configuration string
     * @return raw output bytes
     * @throws Exception if processing fails
     */
    default byte[] execute(byte[] payload, String config) throws Exception {
        String result = execute(toString(payload), config);
        return result != null ? toBytes(result) : null;
    }

    /**
     * Override for text / JSON payloads. Only called via
     * {@link #execute(byte[], String)} — never directly by the worker.
     * <p>
     * The default throws {@link UnsupportedOperationException}. Only override
     * this if your plugin consumes text data; otherwise override
     * {@link #execute(byte[], String)} instead.
     *
     * @param payload input string (typically JSON)
     * @param config  plugin configuration string
     * @return output string
     * @throws Exception if processing fails
     */
    default String execute(String payload, String config) throws Exception {
        throw new UnsupportedOperationException(
                "Plugin does not support text payloads — override execute(byte[], String) instead");
    }

    static String toString(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }

    static byte[] toBytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }
}
