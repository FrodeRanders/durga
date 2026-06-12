package org.gautelis.durga;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ScopeCancellationRegistryTest {

    @Test
    public void shouldTrackCancelledActivities() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A", "activity-B"));

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
        assertFalse(registry.isCancelled("pi-1", "activity-C"));
        assertFalse(registry.isCancelled("pi-2", "activity-A"));
    }

    @Test
    public void shouldClearCancelledActivities() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A", "activity-B"));

        registry.clearScope("pi-1", Set.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
    }

    @Test
    public void shouldMergeCancellationsAcrossCalls() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));
        registry.cancelScope("pi-1", Set.of("activity-B"));

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
        assertTrue(registry.isCancelled("pi-1", "activity-B"));
    }

    @Test
    public void shouldRemoveInstanceEntryWhenAllCleared() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        registry.clearScope("pi-1", List.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullProcessInstanceId() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope(null, Set.of("activity-A"));
        registry.clearScope(null, Set.of("activity-A"));

        assertFalse(registry.isCancelled(null, "activity-A"));
    }

    @Test
    public void shouldIgnoreBlankProcessInstanceId() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("   ", Set.of("activity-A"));
        registry.clearScope("   ", Set.of("activity-A"));

        assertFalse(registry.isCancelled("   ", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullActivityIdInIsCancelled() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        assertFalse(registry.isCancelled("pi-1", null));
        assertFalse(registry.isCancelled("pi-1", ""));
    }

    @Test
    public void shouldIgnoreNullOrEmptyActivityIdsInCancelScope() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", null);
        registry.cancelScope("pi-1", Set.of());

        assertFalse(registry.isCancelled("pi-1", "activity-A"));
    }

    @Test
    public void shouldIgnoreNullOrEmptyActivityIdsInClearScope() {
        ScopeCancellationRegistry registry = new ScopeCancellationRegistry();
        registry.cancelScope("pi-1", Set.of("activity-A"));

        registry.clearScope("pi-1", null);
        registry.clearScope("pi-1", Set.of());

        assertTrue(registry.isCancelled("pi-1", "activity-A"));
    }
}
