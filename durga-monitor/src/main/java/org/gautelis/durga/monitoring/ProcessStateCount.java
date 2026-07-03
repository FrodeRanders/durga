package org.gautelis.durga.monitoring;

/**
 * Count of projections grouped by process id and current state.
 *
 * @param processId process definition identifier
 * @param state current activity or lifecycle state
 * @param count number of instances currently in that state
 */
public record ProcessStateCount(
        String processId,
        String state,
        long count
) {
    /**
     * Parses a materialized count-store key of the form {@code processId:state}.
     *
     * @param stateKey store key
     * @param count count value
     * @return structured count record
     */
    public static ProcessStateCount fromStateKey(String stateKey, long count) {
        String[] parts = stateKey.split(":", 2);
        String processId = parts.length > 0 ? parts[0] : "unknown";
        String state = parts.length > 1 ? parts[1] : "unknown";
        return new ProcessStateCount(processId, state, count);
    }
}
