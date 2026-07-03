package org.gautelis.durga.plugins;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Structured result from a plugin execution, carrying output, idempotency key,
 * error strategy, and optional metadata for lineage and observability.
 */
public final class PluginResult {

    public enum ErrorStrategy {
        FAIL, SKIP, DLQ
    }

    private final byte[] output;
    private final String idempotencyKey;
    private final ErrorStrategy errorStrategy;
    private final String sideEffectDescription;
    private final Map<String, Object> metadata;

    private PluginResult(byte[] output, String idempotencyKey, ErrorStrategy errorStrategy,
                         String sideEffectDescription, Map<String, Object> metadata) {
        this.output = output;
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        this.errorStrategy = errorStrategy;
        this.sideEffectDescription = sideEffectDescription;
        this.metadata = metadata != null ? Collections.unmodifiableMap(new LinkedHashMap<>(metadata))
                : Map.of();
    }

    public byte[] output() {
        return output;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public ErrorStrategy errorStrategy() {
        return errorStrategy;
    }

    public String sideEffectDescription() {
        return sideEffectDescription;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public boolean isSuccess() {
        return errorStrategy == null;
    }

    // ---- factory methods ----

    public static PluginResult success(byte[] output, String idempotencyKey) {
        return new PluginResult(output, idempotencyKey, null, null, Map.of());
    }

    public static PluginResult success(byte[] output, String idempotencyKey, Map<String, Object> metadata) {
        return new PluginResult(output, idempotencyKey, null, null, metadata);
    }

    public static PluginResult success(byte[] output, String idempotencyKey, String sideEffectDescription) {
        return new PluginResult(output, idempotencyKey, null, sideEffectDescription, Map.of());
    }

    public static PluginResult success(byte[] output, String idempotencyKey, String sideEffectDescription,
                                       Map<String, Object> metadata) {
        return new PluginResult(output, idempotencyKey, null, sideEffectDescription, metadata);
    }

    public static PluginResult dlq(byte[] output, String idempotencyKey, String reason) {
        return new PluginResult(output, idempotencyKey, ErrorStrategy.DLQ, reason, Map.of());
    }

    public static PluginResult skip(String idempotencyKey, String reason) {
        return new PluginResult(null, idempotencyKey, ErrorStrategy.SKIP, reason, Map.of());
    }

    public static PluginResult fail(String idempotencyKey, String reason) {
        return new PluginResult(null, idempotencyKey, ErrorStrategy.FAIL, reason, Map.of());
    }
}
