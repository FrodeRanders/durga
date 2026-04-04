package org.gautelis.durga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * Lightweight persisted process state used by generated coordination helpers.
 *
 * @param processInstanceId process instance identifier
 * @param tokens current execution tokens
 * @param variables process-level variables carried between steps
 * @param version monotonic state version written by generated handlers
 */
public record ProcessState(
        String processInstanceId,
        List<Token> tokens,
        Map<String, Object> variables,
        long version
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Minimal execution token used by generated AND-join and subprocess coordination code.
     *
     * @param tokenId token identifier within the process instance
     * @param at activity or scope currently holding the token
     */
    public record Token(String tokenId, String at) {
    }

    /**
     * Serializes this state object as JSON for storage in Kafka.
     *
     * @return JSON representation of this state
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ProcessState", e);
        }
    }

    /**
     * Parses a state snapshot from JSON.
     *
     * @param json JSON produced by {@link #toJson()}
     * @return parsed state
     */
    public static ProcessState fromJson(String json) {
        try {
            return MAPPER.readValue(json, ProcessState.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ProcessState", e);
        }
    }
}
