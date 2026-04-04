package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Query facade over the Kafka Streams monitoring state stores.
 * <p>
 * The service keeps the topology small by deriving some views, such as latency and stuck
 * instances, from the latest-state store at query time instead of materializing dedicated stores.
 */
public final class ProcessMonitoringQueryService {
    private final KafkaStreams streams;
    private final ProcessMonitoringTopology.MonitoringTopics topics;

    /**
     * Creates a query facade bound to a running monitoring topology.
     *
     * @param streams running Kafka Streams instance
     * @param topics monitoring topic and store names
     */
    public ProcessMonitoringQueryService(KafkaStreams streams, ProcessMonitoringTopology.MonitoringTopics topics) {
        this.streams = streams;
        this.topics = topics;
    }

    /**
     * Looks up the latest known view of one process instance.
     *
     * @param processInstanceId process instance identifier
     * @return latest instance view when present
     */
    public Optional<ProcessStateView> findInstance(String processInstanceId) {
        return Optional.ofNullable(stateStore().get(processInstanceId));
    }

    /**
     * Returns counts by current state for one process definition.
     *
     * @param processId process definition identifier
     * @return state counts ordered by state name
     */
    public List<ProcessStateCount> countsForProcess(String processId) {
        List<ProcessStateCount> results = new ArrayList<>();
        // Counts are stored globally by "processId:state", so process-specific views are a
        // lightweight filter over the materialized count store rather than a second projection.
        try (KeyValueIterator<String, Long> iterator = countsStore().all()) {
            while (iterator.hasNext()) {
                var next = iterator.next();
                ProcessStateCount count = ProcessStateCount.fromStateKey(next.key, next.value);
                if (processId.equals(count.processId())) {
                    results.add(count);
                }
            }
        }
        results.sort(Comparator.comparing(ProcessStateCount::state));
        return results;
    }

    /**
     * Returns counts across every process definition currently represented in the count store.
     *
     * @return state counts ordered by process id and state
     */
    public List<ProcessStateCount> allCounts() {
        List<ProcessStateCount> results = new ArrayList<>();
        try (KeyValueIterator<String, Long> iterator = countsStore().all()) {
            while (iterator.hasNext()) {
                var next = iterator.next();
                results.add(ProcessStateCount.fromStateKey(next.key, next.value));
            }
        }
        results.sort(Comparator.comparing(ProcessStateCount::processId).thenComparing(ProcessStateCount::state));
        return results;
    }

    /**
     * Derives per-activity latency summaries for one process definition by scanning the latest
     * state store.
     *
     * @param processId process definition identifier
     * @return latency summaries ordered by activity id
     */
    public List<ActivityLatencySummary> latencyForProcess(String processId) {
        Map<String, ActivityLatencyAccumulator> accumulators = new LinkedHashMap<>();
        // Latency is derived on demand from the latest per-instance projection instead of being
        // maintained as a separate store. That keeps the topology smaller at the cost of scans.
        try (KeyValueIterator<String, ProcessStateView> iterator = stateStore().all()) {
            while (iterator.hasNext()) {
                ProcessStateView state = iterator.next().value;
                if (state == null || !processId.equals(state.processId())) {
                    continue;
                }
                for (Map.Entry<String, Long> entry : state.activityDurationsMs().entrySet()) {
                    accumulators.computeIfAbsent(entry.getKey(), ignored -> new ActivityLatencyAccumulator())
                            .add(entry.getValue());
                }
            }
        }

        List<ActivityLatencySummary> results = new ArrayList<>();
        for (Map.Entry<String, ActivityLatencyAccumulator> entry : accumulators.entrySet()) {
            ActivityLatencyAccumulator value = entry.getValue();
            results.add(new ActivityLatencySummary(
                    processId,
                    entry.getKey(),
                    value.sampleCount,
                    value.averageDurationMs(),
                    value.maxDurationMs
            ));
        }
        results.sort(Comparator.comparing(ActivityLatencySummary::activityId));
        return results;
    }

    /**
     * Finds active instances older than the supplied threshold.
     *
     * @param processId optional process definition filter, or {@code null} for all processes
     * @param olderThanSeconds age threshold in seconds
     * @return stuck instances ordered from oldest to newest
     */
    public List<StuckProcessInstance> stuckInstances(String processId, long olderThanSeconds) {
        Instant now = Instant.now();
        List<StuckProcessInstance> results = new ArrayList<>();
        // "Stuck" is intentionally a query-time notion based on the age of active instances,
        // not a separate lifecycle state written back into the projection.
        try (KeyValueIterator<String, ProcessStateView> iterator = stateStore().all()) {
            while (iterator.hasNext()) {
                ProcessStateView state = iterator.next().value;
                if (state == null || state.lastUpdatedAt() == null || !"active".equals(state.lifecycleState())) {
                    continue;
                }
                if (processId != null && !processId.equals(state.processId())) {
                    continue;
                }
                long ageSeconds = Math.max(0L, Duration.between(Instant.parse(state.lastUpdatedAt()), now).toSeconds());
                if (ageSeconds >= olderThanSeconds) {
                    results.add(StuckProcessInstance.fromState(state, ageSeconds));
                }
            }
        }
        results.sort(Comparator.comparingLong(StuckProcessInstance::ageSeconds).reversed());
        return results;
    }

    private ReadOnlyKeyValueStore<String, ProcessStateView> stateStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.stateStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private ReadOnlyKeyValueStore<String, Long> countsStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.countsStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private static final class ActivityLatencyAccumulator {
        private long sampleCount;
        private long totalDurationMs;
        private long maxDurationMs;

        private void add(long durationMs) {
            sampleCount++;
            totalDurationMs += durationMs;
            maxDurationMs = Math.max(maxDurationMs, durationMs);
        }

        private long averageDurationMs() {
            return sampleCount == 0 ? 0L : totalDurationMs / sampleCount;
        }
    }
}
