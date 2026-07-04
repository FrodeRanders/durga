package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;

public class FaultDetectionTest {

    private static final ProcessEvent SAMPLE_EVENT = new ProcessEvent(
            "pi-1", "order-proc", "validate_order", "tok", "corr",
            Map.of("order_id", 42), ProcessEvent.Status.FAILED,
            new ProcessEvent.ErrorInfo("failed", "TASK_FAILED"),
            ProcessEvent.EventType.PROCESS_FAILED, "v1", "BK", "2026-07-01T10:00:00Z"
    );

    private static final ProcessEvent ESCALATED_EVENT = new ProcessEvent(
            "pi-2", "order-proc", "validate_order", "tok", "corr",
            Map.of("_payloadRedacted", true), ProcessEvent.Status.ESCALATED,
            new ProcessEvent.ErrorInfo("validation failed", "VALIDATION_FAILED"),
            ProcessEvent.EventType.ACTIVITY_ESCALATED, "v1", "BK", "2026-07-01T10:01:00Z"
    );

    // ---- hard error ----

    @Test
    public void shouldFireHardErrorOnFirstOccurrence() {
        System.out.println("TC: HARD_ERROR fires alarm on first matching event");

        AlarmConfig config = new AlarmConfig("cfg-hard", "order-proc", "validate_order",
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.CRITICAL, "Activity ${activityId} failed in process ${processId}", AlarmOrigin.EXPLICIT);

        AlarmEvent alarm = evalHard(config, SAMPLE_EVENT);
        assertNotNull(alarm);
        assertEquals("cfg-hard", alarm.configId());
        assertEquals(AlarmSyndrome.HARD_ERROR, alarm.syndrome());
        assertEquals(AlarmSeverity.CRITICAL, alarm.severity());
        assertEquals("Activity validate_order failed in process order-proc", alarm.message());
        assertEquals("order-proc", alarm.processId());
        assertEquals("validate_order", alarm.activityId());
        assertEquals(ProcessEvent.EventType.PROCESS_FAILED, alarm.triggerEventType());
        assertEquals(1, alarm.count());
    }

    @Test
    public void shouldFireHardErrorRepeatedly() {
        System.out.println("TC: HARD_ERROR fires on every matching event");

        AlarmConfig config = new AlarmConfig("cfg-hard2", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.WARN, "Fault detected", AlarmOrigin.EXPLICIT);

        assertNotNull(evalHard(config, SAMPLE_EVENT));

        ProcessEvent second = new ProcessEvent("pi-2", "other-proc", "other-task", "tok", "corr",
                Map.of(), ProcessEvent.Status.FAILED, new ProcessEvent.ErrorInfo("x", "X"),
                ProcessEvent.EventType.PROCESS_FAILED, "v1", "BK", "2026-07-01T10:02:00Z");
        assertNotNull(evalHard(config, second));
    }

    @Test
    public void shouldNotFireHardErrorForNonMatchingEventType() {
        System.out.println("TC: HARD_ERROR ignores events with wrong event type");

        AlarmConfig config = new AlarmConfig("cfg-hard3", null, null,
                ProcessEvent.EventType.ACTIVITY_ESCALATED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.WARN, "Escalated", AlarmOrigin.EXPLICIT);

        assertNull(evalHard(config, SAMPLE_EVENT));
    }

    @Test
    public void shouldMatchByProcessOnly() {
        System.out.println("TC: config matches any activity in a specific process");

        AlarmConfig config = new AlarmConfig("cfg-proc", "order-proc", null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.WARN, "Process failed", AlarmOrigin.EXPLICIT);

        assertNotNull(evalHard(config, SAMPLE_EVENT));

        ProcessEvent otherProc = new ProcessEvent("pi-3", "other-proc", "validate_order", "tok", "corr",
                Map.of(), ProcessEvent.Status.FAILED, null,
                ProcessEvent.EventType.PROCESS_FAILED, "v1", "BK", "2026-07-01T10:03:00Z");
        assertNull(evalHard(config, otherProc));
    }

    @Test
    public void shouldMatchByActivityOnly() {
        System.out.println("TC: config matches a specific activity in any process");

        AlarmConfig config = new AlarmConfig("cfg-act", null, "validate_order",
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.WARN, "Validation failed", AlarmOrigin.EXPLICIT);

        assertNotNull(evalHard(config, SAMPLE_EVENT));

        ProcessEvent otherAct = new ProcessEvent("pi-4", "order-proc", "other_task", "tok", "corr",
                Map.of(), ProcessEvent.Status.FAILED, null,
                ProcessEvent.EventType.PROCESS_FAILED, "v1", "BK", "2026-07-01T10:04:00Z");
        assertNull(evalHard(config, otherAct));
    }

    // ---- counted ----

    @Test
    public void shouldCountAndFireWhenThresholdExceeded() {
        System.out.println("TC: COUNTED fires alarm only when count exceeds threshold");

        AlarmConfig config = new AlarmConfig("cfg-count", "order-proc", "validate_order",
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.COUNTED, 3, null,
                AlarmSeverity.CRITICAL, "Too many failures: ${count}", AlarmOrigin.EXPLICIT);

        Map<String, Integer> countsStore = new HashMap<>();
        Map<String, Integer> lastAlarmStore = new HashMap<>();

        // 1st failure: count=1, below threshold
        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertEquals(1, (int) countsStore.get("cfg-count:order-proc:validate_order"));

        // 2nd failure: count=2, below threshold
        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertEquals(2, (int) countsStore.get("cfg-count:order-proc:validate_order"));

        // 3rd failure: count=3, at threshold (must exceed)
        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertEquals(3, (int) countsStore.get("cfg-count:order-proc:validate_order"));

        // 4th failure: count=4, exceeds threshold=3 → ALARM
        AlarmEvent alarm = evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore);
        assertNotNull(alarm);
        assertEquals(4, alarm.count());
        assertEquals(3, alarm.threshold());

        // 5th failure: count=5, still above, already fired → suppressed
        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertEquals(5, (int) countsStore.get("cfg-count:order-proc:validate_order"));
    }

    @Test
    public void shouldReArmAfterCountDropsBelowThreshold() {
        System.out.println("TC: COUNTED re-arms alarm after count drops below threshold");

        AlarmConfig config = new AlarmConfig("cfg-rearm", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.COUNTED, 2, null,
                AlarmSeverity.WARN, "${count} faults", AlarmOrigin.EXPLICIT);

        Map<String, Integer> countsStore = new HashMap<>();
        Map<String, Integer> lastAlarmStore = new HashMap<>();

        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
        assertNotNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));

        assertNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));

        // Simulate count dropping back and re-arm
        String key = "cfg-rearm:order-proc:validate_order";
        countsStore.put(key, 2);
        lastAlarmStore.remove(key);

        assertNotNull(evalCounted(config, SAMPLE_EVENT, countsStore, lastAlarmStore));
    }

    // ---- sliding window ----

    @Test
    public void shouldFireWhenWindowCountExceedsThreshold() {
        System.out.println("TC: SLIDING_WINDOW fires alarm when window count > threshold");

        AlarmConfig config = new AlarmConfig("cfg-window", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.SLIDING_WINDOW, 2,
                Duration.ofSeconds(60), AlarmSeverity.WARN, "Window count: ${count}", AlarmOrigin.EXPLICIT);

        Map<String, String> windowsStore = new HashMap<>();
        Map<String, Integer> lastAlarmStore = new HashMap<>();

        assertNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
        assertNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
        assertNotNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
        assertNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
    }

    @Test
    public void shouldPruneOldTimestampsInWindow() {
        System.out.println("TC: SLIDING_WINDOW prunes timestamps older than window duration");

        AlarmConfig config = new AlarmConfig("cfg-prune", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.SLIDING_WINDOW, 2,
                Duration.ofMillis(100), AlarmSeverity.WARN, "Count: ${count}", AlarmOrigin.EXPLICIT);

        Map<String, String> windowsStore = new HashMap<>();
        Map<String, Integer> lastAlarmStore = new HashMap<>();

        evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore);
        evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore);
        assertNotNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
        evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore);

        String key = "cfg-prune:order-proc:validate_order";
        assertNotNull(windowsStore.get(key));

        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        lastAlarmStore.put(key, -1); // reset suppression
        assertNull(evalWindow(config, SAMPLE_EVENT, windowsStore, lastAlarmStore));
    }

    @Test
    public void shouldHandleMultipleConfigs() {
        System.out.println("TC: multiple alarm configs are evaluated independently");

        AlarmConfig hard = new AlarmConfig("cfg-multi-hard", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.CRITICAL, "Hard fault", AlarmOrigin.EXPLICIT);
        AlarmConfig count = new AlarmConfig("cfg-multi-count", null, null,
                ProcessEvent.EventType.PROCESS_FAILED, AlarmSyndrome.COUNTED, 2, null,
                AlarmSeverity.WARN, "Counted fault: ${count}", AlarmOrigin.EXPLICIT);

        // Both match the sample event
        assertNotNull(evalHard(hard, SAMPLE_EVENT));
        assertTrue(count.matches(SAMPLE_EVENT.processId(), SAMPLE_EVENT.activityId()));
        assertEquals(ProcessEvent.EventType.PROCESS_FAILED, SAMPLE_EVENT.eventType());
    }

    @Test
    public void shouldHandleEscalatedEvents() {
        System.out.println("TC: ACTIVITY_ESCALATED triggers configs targeting that event type");

        AlarmConfig config = new AlarmConfig("cfg-esc", null, null,
                ProcessEvent.EventType.ACTIVITY_ESCALATED, AlarmSyndrome.HARD_ERROR, 0, null,
                AlarmSeverity.WARN, "Escalation in ${processId}", AlarmOrigin.EXPLICIT);

        AlarmEvent alarm = evalHard(config, ESCALATED_EVENT);
        assertNotNull(alarm);
        assertEquals("cfg-esc", alarm.configId());
        assertEquals(AlarmSyndrome.HARD_ERROR, alarm.syndrome());
    }

    @Test
    public void shouldBuildTopologyWithRegexEventsAndModelRegistry() {
        System.out.println("TC: fault detection topology builds with regex event subscription and process-model registry");

        org.apache.kafka.streams.Topology topology = FaultDetectionTopology.buildTopology(
                Set.of(),
                FaultDetectionTopology.DEFAULT_EVENTS_PATTERN,
                FaultDetectionTopology.DEFAULT_ALARMS_TOPIC,
                FaultDetectionTopology.DEFAULT_MODELS_TOPIC);

        assertTrue(topology.describe().toString().contains(FaultDetectionTopology.DEFAULT_ALARMS_TOPIC));
    }

    // ---- config validation ----

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectSlidingWindowWithoutDuration() {
        System.out.println("TC: SLIDING_WINDOW config without windowDuration throws");
        new AlarmConfig("bad", null, null, ProcessEvent.EventType.PROCESS_FAILED,
                AlarmSyndrome.SLIDING_WINDOW, 5, null, AlarmSeverity.WARN, "bad", AlarmOrigin.EXPLICIT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectCountedWithoutThreshold() {
        System.out.println("TC: COUNTED config without threshold throws");
        new AlarmConfig("bad", null, null, ProcessEvent.EventType.PROCESS_FAILED,
                AlarmSyndrome.COUNTED, 0, null, AlarmSeverity.WARN, "bad", AlarmOrigin.EXPLICIT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectStuckWithoutWindow() {
        System.out.println("TC: STUCK config without windowDuration (idle timeout) throws");
        new AlarmConfig("bad-stuck", null, null, null, AlarmSyndrome.STUCK,
                0, null, AlarmSeverity.WARN, "bad", AlarmOrigin.AUTOMATIC);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectCascadeWithoutThreshold() {
        System.out.println("TC: CASCADE config without threshold throws");
        new AlarmConfig("bad-cascade", null, null, null, AlarmSyndrome.CASCADE,
                0, Duration.ofSeconds(60), AlarmSeverity.CRITICAL, "bad", AlarmOrigin.AUTOMATIC);
    }

    @Test
    public void shouldRecordOriginLayer() {
        System.out.println("TC: AlarmConfig retains its provenance layer (EXPLICIT / AUTOMATIC)");
        AlarmConfig explicit = new AlarmConfig("x", null, null, ProcessEvent.EventType.PROCESS_FAILED,
                AlarmSyndrome.HARD_ERROR, 0, null, AlarmSeverity.WARN, "m", AlarmOrigin.EXPLICIT);
        assertEquals(AlarmOrigin.EXPLICIT, explicit.origin());

        AlarmConfig auto = new AlarmConfig("y", null, null, null, AlarmSyndrome.STUCK,
                0, Duration.ofSeconds(10), AlarmSeverity.WARN, "m", AlarmOrigin.AUTOMATIC);
        assertEquals(AlarmOrigin.AUTOMATIC, auto.origin());
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullOrigin() {
        System.out.println("TC: AlarmConfig requires an explicit origin layer");
        new AlarmConfig("no-origin", null, null, ProcessEvent.EventType.PROCESS_FAILED,
                AlarmSyndrome.HARD_ERROR, 0, null, AlarmSeverity.WARN, "m", null);
    }

    // ---- stuck / cascade (automatic layer) ----

    @Test
    public void shouldBuildStuckAlarmWithRenderedMessage() {
        System.out.println("TC: STUCK alarm renders instance/activity/idle context");
        AlarmConfig config = new AlarmConfig("builtin:stuck", null, null, null, AlarmSyndrome.STUCK,
                0, Duration.ofSeconds(120), AlarmSeverity.WARN,
                "Stuck ${processInstanceId} in ${processId}/${activityId} for ${idleSeconds}s",
                AlarmOrigin.AUTOMATIC);

        AlarmEvent alarm = FaultDetectionTopology.newStuckAlarm(config, "order-proc", "pi-9",
                "validate_order", ProcessEvent.EventType.ACTIVITY_ESCALATED,
                Instant.now().toString(), 240);

        assertNotNull(alarm);
        assertEquals(AlarmSyndrome.STUCK, alarm.syndrome());
        assertEquals("builtin:stuck", alarm.configId());
        assertEquals("order-proc", alarm.processId());
        assertEquals("pi-9", alarm.processInstanceId());
        assertEquals("validate_order", alarm.activityId());
        assertEquals(ProcessEvent.EventType.ACTIVITY_ESCALATED, alarm.triggerEventType());
        assertEquals(240, alarm.count());
        assertEquals(120, alarm.threshold());
        assertEquals("Stuck pi-9 in order-proc/validate_order for 240s", alarm.message());
    }

    @Test
    public void shouldFireCascadeWhenStuckOnsetsExceedThreshold() {
        System.out.println("TC: CASCADE fires once when stuck onsets in window exceed threshold");
        AlarmConfig config = new AlarmConfig("builtin:cascade", null, null, null, AlarmSyndrome.CASCADE,
                3, Duration.ofSeconds(60), AlarmSeverity.CRITICAL,
                "Cascade: ${count} stuck (threshold ${threshold})", AlarmOrigin.AUTOMATIC);

        Map<String, String> windows = new HashMap<>();
        Map<String, Integer> lastAlarm = new HashMap<>();
        long nowMs = 1_000_000_000L;
        String now = Instant.now().toString();

        assertNull(cascade(config, now, nowMs, windows, lastAlarm));   // 1
        assertNull(cascade(config, now, nowMs, windows, lastAlarm));   // 2
        assertNull(cascade(config, now, nowMs, windows, lastAlarm));   // 3 == threshold
        AlarmEvent alarm = cascade(config, now, nowMs, windows, lastAlarm); // 4 > threshold
        assertNotNull(alarm);
        assertEquals(AlarmSyndrome.CASCADE, alarm.syndrome());
        assertEquals("*", alarm.processId());
        assertNull(alarm.triggerEventType());
        assertEquals(4, alarm.count());
        assertEquals("Cascade: 4 stuck (threshold 3)", alarm.message());

        assertNull(cascade(config, now, nowMs, windows, lastAlarm));   // 5 suppressed
    }

    @Test
    public void shouldReArmCascadeAfterWindowPrunes() {
        System.out.println("TC: CASCADE re-arms after onsets age out of the window");
        AlarmConfig config = new AlarmConfig("builtin:cascade", null, null, null, AlarmSyndrome.CASCADE,
                2, Duration.ofSeconds(60), AlarmSeverity.CRITICAL, "Cascade ${count}", AlarmOrigin.AUTOMATIC);

        Map<String, String> windows = new HashMap<>();
        Map<String, Integer> lastAlarm = new HashMap<>();
        long nowMs = 5_000_000_000L;
        String now = Instant.now().toString();

        cascade(config, now, nowMs, windows, lastAlarm);
        cascade(config, now, nowMs, windows, lastAlarm);
        assertNotNull(cascade(config, now, nowMs, windows, lastAlarm)); // fired
        assertNotNull(lastAlarm.get(FaultDetectionTopology.cascadeAlarmKey(config)));

        FaultDetectionTopology.maintainCascade(config, nowMs + 120_000L,
                new MapKVStore<>(windows), new MapKVStore<>(lastAlarm));
        assertNull(lastAlarm.get(FaultDetectionTopology.cascadeAlarmKey(config)));
    }

    @Test
    public void shouldTreatOnlyProcessTerminalsAsTerminal() {
        System.out.println("TC: only PROCESS_COMPLETED / PROCESS_FAILED end stall tracking");
        assertTrue(FaultDetectionTopology.isTerminal(ProcessEvent.EventType.PROCESS_COMPLETED));
        assertTrue(FaultDetectionTopology.isTerminal(ProcessEvent.EventType.PROCESS_FAILED));
        assertFalse(FaultDetectionTopology.isTerminal(ProcessEvent.EventType.ACTIVITY_ESCALATED));
        assertFalse(FaultDetectionTopology.isTerminal(ProcessEvent.EventType.ACTIVITY_ENTERED));
    }

    @Test
    public void shouldBuildTopologyWithBuiltInStuckAndCascade() {
        System.out.println("TC: fault detection topology builds with automatic stuck + cascade configs");
        AlarmConfig stuck = new AlarmConfig("builtin:stuck", null, null, null, AlarmSyndrome.STUCK,
                0, Duration.ofSeconds(120), AlarmSeverity.WARN, "stuck", AlarmOrigin.AUTOMATIC);
        AlarmConfig cascade = new AlarmConfig("builtin:cascade", null, null, null, AlarmSyndrome.CASCADE,
                5, Duration.ofSeconds(60), AlarmSeverity.CRITICAL, "cascade", AlarmOrigin.AUTOMATIC);

        org.apache.kafka.streams.Topology topology = FaultDetectionTopology.buildTopology(
                Set.of(stuck, cascade),
                FaultDetectionTopology.DEFAULT_EVENTS_PATTERN,
                FaultDetectionTopology.DEFAULT_ALARMS_TOPIC,
                FaultDetectionTopology.DEFAULT_MODELS_TOPIC);

        assertTrue(topology.describe().toString().contains(FaultDetectionTopology.DEFAULT_ALARMS_TOPIC));
    }

    @Test
    public void shouldScopeCascadeAlarmToProcessWhenConfigured() {
        System.out.println("TC: per-process CASCADE alarm carries the process scope, not the system wildcard");
        AlarmConfig config = new AlarmConfig("pipeline:surge", "pipeline", null, null, AlarmSyndrome.CASCADE,
                4, Duration.ofSeconds(30), AlarmSeverity.CRITICAL,
                "${count} instances stalled in ${processId}", AlarmOrigin.EXPLICIT);

        AlarmEvent alarm = FaultDetectionTopology.newCascadeAlarm(config, Instant.now().toString(), 5);
        assertEquals("pipeline", alarm.processId());
        assertEquals("pipeline:surge", alarm.configId());
        assertEquals("5 instances stalled in pipeline", alarm.message());
    }

    @Test
    public void shouldKeepCascadeWindowsIsolatedPerConfig() {
        System.out.println("TC: built-in and per-process cascade configs use distinct window/alarm keys");
        AlarmConfig builtin = new AlarmConfig("builtin:cascade", null, null, null, AlarmSyndrome.CASCADE,
                5, Duration.ofSeconds(60), AlarmSeverity.CRITICAL, "c", AlarmOrigin.AUTOMATIC);
        AlarmConfig perProcess = new AlarmConfig("pipeline:surge", "pipeline", null, null, AlarmSyndrome.CASCADE,
                4, Duration.ofSeconds(30), AlarmSeverity.CRITICAL, "c", AlarmOrigin.EXPLICIT);

        assertNotEquals(FaultDetectionTopology.cascadeAlarmKey(builtin),
                FaultDetectionTopology.cascadeAlarmKey(perProcess));
        assertNotEquals(FaultDetectionTopology.cascadeOnsetKey(builtin),
                FaultDetectionTopology.cascadeOnsetKey(perProcess));
    }

    // ---- helpers ----

    private static AlarmEvent cascade(AlarmConfig config, String now, long nowMs,
                                       Map<String, String> windowsStore,
                                       Map<String, Integer> lastAlarmStore) {
        return FaultDetectionTopology.recordCascadeOnset(config, now, nowMs,
                new MapKVStore<>(windowsStore), new MapKVStore<>(lastAlarmStore));
    }

    private static AlarmEvent evalHard(AlarmConfig config, ProcessEvent event) {
        if (event.eventType() != config.eventType()) return null;
        if (!config.matches(event.processId(), event.activityId())) return null;
        return FaultDetectionTopology.newAlarm(config, event, Instant.now().toString(), 1);
    }

    private static AlarmEvent evalCounted(AlarmConfig config, ProcessEvent event,
                                           Map<String, Integer> countsStore,
                                           Map<String, Integer> lastAlarmStore) {
        return FaultDetectionTopology.evaluateCounted(config, event, Instant.now().toString(),
                new MapKVStore<>(countsStore), new MapKVStore<>(lastAlarmStore));
    }

    private static AlarmEvent evalWindow(AlarmConfig config, ProcessEvent event,
                                          Map<String, String> windowsStore,
                                          Map<String, Integer> lastAlarmStore) {
        return FaultDetectionTopology.evaluateSlidingWindow(config, event, Instant.now().toString(),
                new MapKVStore<>(windowsStore), new MapKVStore<>(lastAlarmStore));
    }

    /**
     * Wraps a HashMap as a KeyValueStore. Only get/put/delete are used by the fault detector.
     */
    @SuppressWarnings("unchecked")
    static class MapKVStore<V> implements org.apache.kafka.streams.state.KeyValueStore<String, V> {
        private final Map<String, V> map;

        MapKVStore(Map<String, V> map) { this.map = map; }

        @Override public void put(String key, V value) { map.put(key, value); }
        @Override public V putIfAbsent(String key, V value) { return map.putIfAbsent(key, value); }
        @Override public void putAll(List<org.apache.kafka.streams.KeyValue<String, V>> entries) {
            for (var e : entries) map.put(e.key, e.value);
        }
        @Override public V delete(String key) { return map.remove(key); }
        @Override public V get(String key) { return map.get(key); }
        @Override
        public org.apache.kafka.streams.state.KeyValueIterator<String, V> range(String from, String to) {
            throw new UnsupportedOperationException();
        }
        @Override
        public org.apache.kafka.streams.state.KeyValueIterator<String, V> reverseRange(String from, String to) {
            throw new UnsupportedOperationException();
        }
        @Override
        public org.apache.kafka.streams.state.KeyValueIterator<String, V> all() {
            throw new UnsupportedOperationException();
        }
        @Override
        public org.apache.kafka.streams.state.KeyValueIterator<String, V> reverseAll() {
            throw new UnsupportedOperationException();
        }
        @Override
        public long approximateNumEntries() { return map.size(); }
        @Override public String name() { return "map-" + map.hashCode(); }
        @Override
        public void init(org.apache.kafka.streams.processor.StateStoreContext context, org.apache.kafka.streams.processor.StateStore root) {
        }
        @Override public void flush() {}
        @Override public boolean persistent() { return false; }
        @Override public boolean isOpen() { return true; }
        @Override public void close() {}
    }
}
