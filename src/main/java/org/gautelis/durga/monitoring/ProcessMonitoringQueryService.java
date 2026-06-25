package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Query facade over the Kafka Streams monitoring state stores.
 * <p>
 * The service reads from replicated query stores fed by monitoring topics, so each monitoring app
 * instance has a full local copy of current state, counts, latency summaries, active-instance
 * indexes, and coarse trend buckets.
 */
public final class ProcessMonitoringQueryService {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitoringQueryService.class);

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
        try (KeyValueIterator<String, ProcessStateCount> iterator = countsStore().all()) {
            while (iterator.hasNext()) {
                ProcessStateCount count = iterator.next().value;
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
        try (KeyValueIterator<String, ProcessStateCount> iterator = countsStore().all()) {
            while (iterator.hasNext()) {
                results.add(iterator.next().value);
            }
        }
        results.sort(Comparator.comparing(ProcessStateCount::processId).thenComparing(ProcessStateCount::state));
        return results;
    }

    /**
     * Returns all known process IDs from the count store.
     */
    public List<String> listProcessIds() {
        java.util.LinkedHashSet<String> ids = new java.util.LinkedHashSet<>();
        try (KeyValueIterator<String, ProcessStateCount> iterator = countsStore().all()) {
            while (iterator.hasNext()) {
                ProcessStateCount count = iterator.next().value;
                if (count.processId() != null && !count.processId().isBlank()) {
                    ids.add(count.processId());
                }
            }
        }
        List<String> sorted = new ArrayList<>(ids);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

    /**
     * Returns pre-aggregated per-activity latency summaries for one process definition.
     *
     * @param processId process definition identifier
     * @return latency summaries ordered by activity id
     */
    public List<ActivityLatencySummary> latencyForProcess(String processId) {
        List<ActivityLatencySummary> results = new ArrayList<>();
        try (KeyValueIterator<String, ActivityLatencySummary> iterator = latencyStore().all()) {
            while (iterator.hasNext()) {
                ActivityLatencySummary summary = iterator.next().value;
                if (summary != null && processId.equals(summary.processId())) {
                    results.add(summary);
                }
            }
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
        // "Stuck" remains a thresholded query, but it now scans only the dedicated active index
        // rather than the full latest-state store.
        try (KeyValueIterator<String, ProcessStateView> iterator = activeStore().all()) {
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

    /**
     * Returns coarse lifecycle trend buckets for one process definition.
     *
     * @param processId process definition identifier
     * @return trend points ordered by bucket start and metric
     */
    public List<ProcessTrendPoint> trendsForProcess(String processId) {
        List<ProcessTrendPoint> results = new ArrayList<>();
        try (KeyValueIterator<String, ProcessTrendPoint> iterator = trendsStore().all()) {
            while (iterator.hasNext()) {
                ProcessTrendPoint trend = iterator.next().value;
                if (trend != null && processId.equals(trend.processId())) {
                    results.add(trend);
                }
            }
        }
        results.sort(Comparator.comparing(ProcessTrendPoint::bucketStartedAt).thenComparing(ProcessTrendPoint::metric));
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

    private ReadOnlyKeyValueStore<String, ProcessStateCount> countsStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.countsStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private ReadOnlyKeyValueStore<String, ProcessStateView> activeStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.activeStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private ReadOnlyKeyValueStore<String, ActivityLatencySummary> latencyStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.latencyStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private ReadOnlyKeyValueStore<String, ProcessTrendPoint> trendsStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        topics.trendsStore(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }
}
