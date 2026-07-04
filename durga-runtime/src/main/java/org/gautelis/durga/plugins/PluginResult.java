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

    /**
     * Describes how the generated worker should treat this result's {@link #output()} relative to
     * the task's input payload.
     */
    public enum OutputDisposition {
        /** Output is the new payload and replaces the input payload downstream. */
        PAYLOAD,
        /** Input payload is forwarded unchanged; output is a control/annotation value (e.g. a route key or inspection result). */
        PASSTHROUGH,
        /** Input payload is forwarded unchanged; output describes an external side effect (e.g. a stored-object handle). */
        SIDE_EFFECT
    }

    private final byte[] output;
    private final String idempotencyKey;
    private final ErrorStrategy errorStrategy;
    private final OutputDisposition disposition;
    private final String sideEffectDescription;
    private final Map<String, Object> metadata;

    private PluginResult(byte[] output, String idempotencyKey, ErrorStrategy errorStrategy,
                         OutputDisposition disposition, String sideEffectDescription,
                         Map<String, Object> metadata) {
        this.output = output;
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        this.errorStrategy = errorStrategy;
        this.disposition = disposition != null ? disposition : OutputDisposition.PAYLOAD;
        this.sideEffectDescription = sideEffectDescription;
        this.metadata = metadata != null ? Collections.unmodifiableMap(new LinkedHashMap<>(metadata))
                : Map.of();
    }

    public byte[] output() {
        return output;
    }

    /**
     * How the worker should treat {@link #output()}. Defaults to {@link OutputDisposition#PAYLOAD}.
     */
    public OutputDisposition disposition() {
        return disposition;
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
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PAYLOAD, null, Map.of());
    }

    public static PluginResult success(byte[] output, String idempotencyKey, Map<String, Object> metadata) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PAYLOAD, null, metadata);
    }

    public static PluginResult success(byte[] output, String idempotencyKey, String sideEffectDescription) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PAYLOAD, sideEffectDescription, Map.of());
    }

    public static PluginResult success(byte[] output, String idempotencyKey, String sideEffectDescription,
                                       Map<String, Object> metadata) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PAYLOAD, sideEffectDescription, metadata);
    }

    /**
     * Successful result whose output is a control/annotation value (route key, inspection result):
     * the input payload is forwarded unchanged.
     */
    public static PluginResult passthrough(byte[] output, String idempotencyKey) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PASSTHROUGH, null, Map.of());
    }

    public static PluginResult passthrough(byte[] output, String idempotencyKey, Map<String, Object> metadata) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.PASSTHROUGH, null, metadata);
    }

    /**
     * Successful result whose output describes an external side effect (e.g. a stored-object handle):
     * the input payload is forwarded unchanged.
     */
    public static PluginResult sideEffect(byte[] output, String idempotencyKey, String sideEffectDescription) {
        return new PluginResult(output, idempotencyKey, null, OutputDisposition.SIDE_EFFECT, sideEffectDescription, Map.of());
    }

    public static PluginResult dlq(byte[] output, String idempotencyKey, String reason) {
        return new PluginResult(output, idempotencyKey, ErrorStrategy.DLQ, OutputDisposition.PAYLOAD, reason, Map.of());
    }

    public static PluginResult skip(String idempotencyKey, String reason) {
        return new PluginResult(null, idempotencyKey, ErrorStrategy.SKIP, OutputDisposition.PAYLOAD, reason, Map.of());
    }

    public static PluginResult fail(String idempotencyKey, String reason) {
        return new PluginResult(null, idempotencyKey, ErrorStrategy.FAIL, OutputDisposition.PAYLOAD, reason, Map.of());
    }
}
