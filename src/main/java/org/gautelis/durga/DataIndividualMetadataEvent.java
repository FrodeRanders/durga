package org.gautelis.durga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata/provenance event compatible with Vannak's data-individual model.
 * <p>
 * Durga emits these as companion events to process lifecycle events when a worker can identify
 * a concrete flowing data item and the metadata produced around it.
 */
public record DataIndividualMetadataEvent(
        String metadataEventId,
        String dataIndividualId,
        BigInteger dataIndividualShardId,
        String tenantId,
        String environmentId,
        String pipelineId,
        String processInstanceId,
        String activityId,
        String timestamp,
        Operation operation,
        Map<String, Object> passiveMetadata,
        Map<String, Object> activeMetadata,
        String sourcePayloadRef,
        String idempotencyKey
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Operation {
        CREATED,
        RECEIVED,
        TRANSFORMED,
        MASKED,
        VALIDATED,
        ENRICHED,
        ROUTED,
        PERSISTED
    }

    public DataIndividualMetadataEvent {
        timestamp = timestamp != null ? timestamp : Instant.now().toString();
        passiveMetadata = immutableNullableMap(passiveMetadata);
        activeMetadata = immutableNullableMap(activeMetadata);
        dataIndividualId = dataIndividualId != null ? dataIndividualId : processInstanceId;
        metadataEventId = metadataEventId != null ? metadataEventId
                : defaultMetadataEventId(dataIndividualId, processInstanceId, activityId, operation, timestamp);
        idempotencyKey = idempotencyKey != null ? idempotencyKey
                : dataIndividualId + ":" + metadataEventId;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize DataIndividualMetadataEvent", e);
        }
    }

    public static DataIndividualMetadataEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, DataIndividualMetadataEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse DataIndividualMetadataEvent", e);
        }
    }

    public static BigInteger shardIdFor(String dataIndividualId) {
        if (dataIndividualId == null) {
            return BigInteger.ZERO;
        }
        long hash = 0xcbf29ce484222325L;
        for (byte b : dataIndividualId.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            hash ^= Byte.toUnsignedLong(b);
            hash *= 0x00000100000001b3L;
        }
        byte[] bytes = java.nio.ByteBuffer.allocate(Long.BYTES).putLong(hash).array();
        return new BigInteger(1, bytes);
    }

    private static String defaultMetadataEventId(
            String dataIndividualId,
            String processInstanceId,
            String activityId,
            Operation operation,
            String timestamp
    ) {
        String basis = String.join(":",
                valueOrEmpty(dataIndividualId),
                valueOrEmpty(processInstanceId),
                valueOrEmpty(activityId),
                operation != null ? operation.name() : "",
                valueOrEmpty(timestamp));
        return java.util.UUID.nameUUIDFromBytes(basis.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }

    private static String valueOrEmpty(String value) {
        return value != null ? value : "";
    }

    private static Map<String, Object> immutableNullableMap(Map<String, Object> value) {
        if (value == null || value.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(value));
    }
}
