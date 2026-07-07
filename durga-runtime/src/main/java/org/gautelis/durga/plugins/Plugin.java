package org.gautelis.durga.plugins;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Contract for data pipeline plugins.
 * <p>
 * Plugins receive a raw payload and a configuration string, and return a
 * transformed payload. If the transformation fails, they throw.
 * <p>
 * The generated worker always calls {@link #executeWithResult(byte[], String, PluginExecutionContext)}.
 * <ul>
 * <li><b>Text / JSON plugins</b> — override {@link #execute(String, String)}.
 *     The default {@code byte[]} overload handles UTF-8 conversion.</li>
 * <li><b>Binary plugins</b> — override {@link #execute(byte[], String)} directly.</li>
 * <li><b>Idempotency-aware plugins</b> — override
 *     {@link #executeWithResult(byte[], String)} to return structured
 *     {@link PluginResult} instances with idempotency keys and error strategies.</li>
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

    /**
     * Structured execution that returns a {@link PluginResult} with idempotency
     * key and optional metadata. The generated worker always calls this method,
     * passing {@link PluginExecutionContext#production()} in a normal build and
     * {@link PluginExecutionContext#validation()} in a validation-mode build.
     * <p>
     * The default implementation delegates to {@link #execute(byte[], String)}
     * and wraps the result with a content-based idempotency key, ignoring the
     * context (safe for pure transforms).
     * <p>
     * Override this method directly if your plugin needs to:
     * <ul>
     * <li>Declare an explicit idempotency strategy</li>
     * <li>Return side-effect metadata</li>
     * <li>Signal a non-exceptional error strategy (DLQ, skip)</li>
     * <li>Suppress substantial side effects when
     *     {@link PluginExecutionContext#validationMode()} is {@code true}</li>
     * </ul>
     *
     * @param payload raw input bytes
     * @param config  plugin configuration string
     * @param context the execution context (never {@code null})
     * @return structured plugin result
     * @throws Exception if processing fails and the error strategy is FAIL
     */
    default PluginResult executeWithResult(byte[] payload, String config, PluginExecutionContext context)
            throws Exception {
        byte[] output = execute(payload, config);
        return PluginResult.success(output, idempotencyKey(payload, config));
    }

    /**
     * Generates a content-based idempotency key from the input payload and config.
     * <p>
     * Override this method if your plugin needs a specific key strategy
     * (e.g. extracting a business key from the payload instead of hashing bytes).
     *
     * @param payload raw input bytes
     * @param config  plugin configuration string
     * @return an idempotency key string
     */
    default String idempotencyKey(byte[] payload, String config) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(payload != null ? payload : new byte[0]);
            if (config != null) {
                digest.update(config.getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static String toString(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }

    static byte[] toBytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }
}
