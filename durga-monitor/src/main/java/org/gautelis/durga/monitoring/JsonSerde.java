package org.gautelis.durga.monitoring;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Small JSON serde factory for Kafka Streams projections and query models.
 */
public final class JsonSerde {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSerde() {
    }

    /**
     * Creates a JSON-backed serde for the supplied type.
     *
     * @param type value class to serialize and deserialize
     * @param <T> value type
     * @return serde for the supplied type
     */
    public static <T> Serde<T> forClass(Class<T> type) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to serialize " + type.getSimpleName(), e);
            }
        };

        Deserializer<T> deserializer = (topic, data) -> {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return MAPPER.readValue(data, type);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to deserialize " + type.getSimpleName(), e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
