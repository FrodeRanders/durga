package org.gautelis.durga;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cancellation registry used by generated handlers to suppress late normal-flow work
 * after interrupting boundaries or event subprocesses have taken over a scope.
 */
@ApplicationScoped
public class ScopeCancellationRegistry {
    // Generated handlers use this as a lightweight in-memory suppression registry so interrupting
    // boundaries and event subprocesses can block late normal-flow progress in the same runtime.
    private final Map<String, Set<String>> cancelledActivitiesByInstance = new ConcurrentHashMap<>();

    /**
     * Marks a set of activities or scopes as cancelled for a process instance.
     *
     * @param processInstanceId process instance identifier
     * @param activityIds activity or scope ids to suppress
     */
    public void cancelScope(String processInstanceId, Collection<String> activityIds) {
        if (processInstanceId == null || processInstanceId.isBlank() || activityIds == null || activityIds.isEmpty()) {
            return;
        }
        cancelledActivitiesByInstance.compute(processInstanceId, (ignored, existing) -> {
            Set<String> updated = existing != null ? ConcurrentHashMap.newKeySet(existing.size() + activityIds.size())
                    : ConcurrentHashMap.newKeySet();
            if (existing != null) {
                updated.addAll(existing);
            }
            updated.addAll(activityIds);
            return updated;
        });
    }

    /**
     * Clears cancellation markers when a scope is explicitly reset or completed.
     *
     * @param processInstanceId process instance identifier
     * @param activityIds activity or scope ids to remove from the registry
     */
    public void clearScope(String processInstanceId, Collection<String> activityIds) {
        if (processInstanceId == null || processInstanceId.isBlank() || activityIds == null || activityIds.isEmpty()) {
            return;
        }
        cancelledActivitiesByInstance.computeIfPresent(processInstanceId, (ignored, existing) -> {
            existing.removeAll(activityIds);
            return existing.isEmpty() ? null : existing;
        });
    }

    /**
     * Checks whether an activity or scope has been cancelled for a process instance.
     *
     * @param processInstanceId process instance identifier
     * @param activityId activity or scope id
     * @return {@code true} if normal progress should be suppressed
     */
    public boolean isCancelled(String processInstanceId, String activityId) {
        if (processInstanceId == null || processInstanceId.isBlank() || activityId == null || activityId.isBlank()) {
            return false;
        }
        Set<String> cancelled = cancelledActivitiesByInstance.get(processInstanceId);
        return cancelled != null && cancelled.contains(activityId);
    }
}
