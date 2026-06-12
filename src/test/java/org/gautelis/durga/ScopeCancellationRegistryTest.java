package org.gautelis.durga;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ScopeCancellationRegistryTest {

    @Test
    public void shouldTrackCancelledActivities() {
        System.out.println("TC: tracks cancelled activities per process instance and returns correct isCancelled");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A", "activity-B"));

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
        assertFalse(registry.isCancelled("pi-1", "activity-C"));
        assertFalse(registry.isCancelled("pi-2", "activity-A"));
    }

    @Test
    public void shouldClearCancelledActivities() {
        System.out.println("TC: clears individual cancelled activities while keeping others intact");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A", "activity-B"));

        registry.clearScope("pi-1", Set.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
    }

    @Test
    public void shouldMergeCancellationsAcrossCalls() {
        System.out.println("TC: merges cancellation sets across multiple cancelScope calls for the same instance");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));
        registry.cancelScope("pi-1", Set.of("activity-B"));

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
    }

    @Test
    public void shouldRemoveInstanceEntryWhenAllCleared() {
        System.out.println("TC: removes instance entry when all cancelled activities are cleared");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        registry.clearScope("pi-1", List.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullProcessInstanceId() {
        System.out.println("TC: ignores null processInstanceId in cancelScope, clearScope and isCancelled");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope(null, Set.of("activity-A"));
        registry.clearScope(null, Set.of("activity-A"));

        assertFalse(registry.isCancelled(null, "activity-A"));
    }

    @Test
    public void shouldIgnoreBlankProcessInstanceId() {
        System.out.println("TC: ignores blank processInstanceId in cancelScope, clearScope and isCancelled");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("   ", Set.of("activity-A"));
        registry.clearScope("   ", Set.of("activity-A"));

        assertFalse(registry.isCancelled("   ", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullActivityIdInIsCancelled() {
        System.out.println("TC: returns false for null or empty activityId in isCancelled");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", null));
        assertFalse(registry.isCancelled("pi-1", ""));
    }

    @Test
    public void shouldIgnoreNullOrEmptyActivityIdsInCancelScope() {
        System.out.println("TC: ignores null or empty activityIds set in cancelScope");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", null);
        registry.cancelScope("pi-1", Set.of());

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullOrEmptyActivityIdsInClearScope() {
        System.out.println("TC: ignores null or empty activityIds in clearScope and preserves existing cancellations");
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        registry.clearScope("pi-1", null);
        registry.clearScope("pi-1", Set.of());

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
    }
}
