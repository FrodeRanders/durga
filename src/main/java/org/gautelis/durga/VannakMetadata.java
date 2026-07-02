package org.gautelis.durga;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helper methods for creating Vannak-compatible data-individual metadata events.
 */
public final class VannakMetadata {
    private VannakMetadata() {
    }

    public static DataIndividualMetadataEvent pluginEvent(
            ProcessEvent inputEvent,
            String activityId,
            String pluginId,
            String pluginConfig,
            String inputPayloadJson,
            Map<String, Object> outputPayload
    ) {
        String dataIndividualId = dataIndividualId(inputEvent, outputPayload);
        Map<String, Object> passive = new LinkedHashMap<>();
        passive.put("durga:processId", inputEvent.processId());
        passive.put("durga:processInstanceId", inputEvent.processInstanceId());
        passive.put("durga:activityId", activityId);
        passive.put("durga:businessKey", inputEvent.businessKey());
        passive.put("durga:correlationId", inputEvent.correlationId());
        passive.put("durga:inputBytes", inputPayloadJson != null
                ? inputPayloadJson.getBytes(StandardCharsets.UTF_8).length : 0);
        passive.put("durga:inputSha256", sha256(inputPayloadJson));

        Map<String, Object> active = new LinkedHashMap<>();
        active.put("durga:plugin", pluginId);
        active.put("durga:pluginConfig", pluginConfig);
        if (outputPayload != null) {
            active.put("durga:outputFields", outputPayload.keySet().stream().sorted().toList());
            copyNested(outputPayload, active, "format", "durga:format");
            copyNested(outputPayload, active, "dataHandle", "durga:dataHandle");
        }

        String sourcePayloadRef = sourcePayloadRef(outputPayload);
        String now = Instant.now().toString();
        String metadataEventId = stableEventId(dataIndividualId, inputEvent.processInstanceId(),
                activityId, pluginId, now);
        return new DataIndividualMetadataEvent(
                metadataEventId,
                dataIndividualId,
                DataIndividualMetadataEvent.shardIdFor(dataIndividualId),
                tenantId(inputEvent),
                environmentId(inputEvent),
                inputEvent.processId(),
                inputEvent.processInstanceId(),
                activityId,
                now,
                operationFor(pluginId),
                passive,
                active,
                sourcePayloadRef,
                dataIndividualId + ":" + metadataEventId
        );
    }

    private static DataIndividualMetadataEvent.Operation operationFor(String pluginId) {
        if (pluginId == null) {
            return DataIndividualMetadataEvent.Operation.TRANSFORMED;
        }
        if (pluginId.contains("mask")) {
            return DataIndividualMetadataEvent.Operation.MASKED;
        }
        if (pluginId.contains("validator") || pluginId.contains("validate")) {
            return DataIndividualMetadataEvent.Operation.VALIDATED;
        }
        if (pluginId.contains("enrich")) {
            return DataIndividualMetadataEvent.Operation.ENRICHED;
        }
        if (pluginId.contains("router") || pluginId.contains("route")) {
            return DataIndividualMetadataEvent.Operation.ROUTED;
        }
        if (pluginId.contains("collector") || pluginId.contains("extractor")) {
            return DataIndividualMetadataEvent.Operation.PERSISTED;
        }
        return DataIndividualMetadataEvent.Operation.TRANSFORMED;
    }

    @SuppressWarnings("unchecked")
    private static String dataIndividualId(ProcessEvent inputEvent, Map<String, Object> outputPayload) {
        Object explicit = firstNonNull(
                nested(outputPayload, "dataIndividualId"),
                nested(outputPayload, "data_individual_id"),
                nested(outputPayload, "id"),
                nested(inputEvent.payload(), "dataIndividualId"),
                nested(inputEvent.payload(), "data_individual_id"),
                nested(inputEvent.payload(), "id"),
                inputEvent.businessKey(),
                inputEvent.correlationId(),
                inputEvent.processInstanceId());
        if (explicit instanceof Map<?, ?> map && map.get("uri") != null) {
            return String.valueOf(map.get("uri"));
        }
        return String.valueOf(explicit);
    }

    private static String tenantId(ProcessEvent inputEvent) {
        Object tenant = nested(inputEvent.payload(), "tenantId");
        return tenant != null ? String.valueOf(tenant) : "default";
    }

    private static String environmentId(ProcessEvent inputEvent) {
        Object environment = nested(inputEvent.payload(), "environmentId");
        return environment != null ? String.valueOf(environment) : "default";
    }

    private static Object nested(Map<String, Object> map, String path) {
        if (map == null || path == null) {
            return null;
        }
        Object current = map;
        for (String segment : path.split("\\.")) {
            if (current instanceof Map<?, ?> nestedMap) {
                current = nestedMap.get(segment);
            } else {
                return null;
            }
        }
        return current;
    }

    private static Object firstNonNull(Object... values) {
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static void copyNested(
            Map<String, Object> source,
            Map<String, Object> target,
            String sourceField,
            String targetField
    ) {
        Object value = source.get(sourceField);
        if (value != null) {
            target.put(targetField, value);
        }
    }

    @SuppressWarnings("unchecked")
    private static String sourcePayloadRef(Map<String, Object> outputPayload) {
        Object handle = nested(outputPayload, "dataHandle");
        if (handle instanceof Map<?, ?> map && map.get("uri") != null) {
            return String.valueOf(map.get("uri"));
        }
        Object payloadRef = nested(outputPayload, "payloadRef");
        return payloadRef != null ? String.valueOf(payloadRef) : null;
    }

    private static String stableEventId(
            String dataIndividualId,
            String processInstanceId,
            String activityId,
            String pluginId,
            String timestamp
    ) {
        String basis = String.join(":",
                valueOrEmpty(dataIndividualId),
                valueOrEmpty(processInstanceId),
                valueOrEmpty(activityId),
                valueOrEmpty(pluginId),
                valueOrEmpty(timestamp));
        return java.util.UUID.nameUUIDFromBytes(basis.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static String sha256(String value) {
        if (value == null) {
            return "";
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String valueOrEmpty(String value) {
        return value != null ? value : "";
    }
}
