package org.gautelis.durga.monitoring;

/**
 * Query result representing an instance that has stayed active longer than a caller-supplied
 * threshold.
 *
 * @param processInstanceId process instance identifier
 * @param processId process definition identifier
 * @param currentActivityId current activity or scope
 * @param lifecycleState current lifecycle state
 * @param lastUpdatedAt timestamp of the last observed lifecycle event
 * @param ageSeconds age since last update in seconds
 */
public record StuckProcessInstance(
        String processInstanceId,
        String processId,
        String currentActivityId,
        String lifecycleState,
        String lastUpdatedAt,
        long ageSeconds
) {
    /**
     * Converts a materialized instance view into a stuck-instance result.
     *
     * @param state latest instance view
     * @param ageSeconds derived age in seconds
     * @return stuck-instance result
     */
    public static StuckProcessInstance fromState(ProcessStateView state, long ageSeconds) {
        return new StuckProcessInstance(
                state.processInstanceId(),
                state.processId(),
                state.currentActivityId(),
                state.lifecycleState(),
                state.lastUpdatedAt(),
                ageSeconds
        );
    }
}
