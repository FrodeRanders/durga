package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.gautelis.durga.validation.ValidationResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Query facade over the validation results read model.
 * <p>
 * Reads the replicated {@code validation-results} global store, which holds the latest comparison
 * of each candidate output against its prior/production output, keyed by
 * {@code processId:taskId:processInstanceId}.
 */
public final class ValidationQueryService {

    private final KafkaStreams streams;
    private final String resultsStore;

    public ValidationQueryService(KafkaStreams streams, String resultsStore) {
        this.streams = streams;
        this.resultsStore = resultsStore;
    }

    /**
     * Returns validation results, optionally filtered by process, task, and match status.
     *
     * @param processId optional process filter, or {@code null}
     * @param taskId optional task filter, or {@code null}
     * @param status optional match-status filter (case-insensitive), or {@code null}
     * @return matching results, newest first
     */
    public List<ValidationResult> results(String processId, String taskId, String status) {
        ValidationResult.MatchStatus statusFilter = parseStatus(status);
        List<ValidationResult> results = new ArrayList<>();
        try (KeyValueIterator<String, ValidationResult> iterator = store().all()) {
            while (iterator.hasNext()) {
                ValidationResult result = iterator.next().value;
                if (result == null) {
                    continue;
                }
                if (processId != null && !processId.equals(result.processId())) {
                    continue;
                }
                if (taskId != null && !taskId.equals(result.taskId())) {
                    continue;
                }
                if (statusFilter != null && result.matchStatus() != statusFilter) {
                    continue;
                }
                results.add(result);
            }
        }
        results.sort(Comparator.comparing(ValidationResult::timestamp,
                Comparator.nullsLast(Comparator.reverseOrder())));
        return results;
    }

    /**
     * Returns the results for one instance across all tasks under validation.
     */
    public List<ValidationResult> resultsForInstance(String processInstanceId) {
        List<ValidationResult> results = new ArrayList<>();
        try (KeyValueIterator<String, ValidationResult> iterator = store().all()) {
            while (iterator.hasNext()) {
                ValidationResult result = iterator.next().value;
                if (result != null && processInstanceId.equals(result.processInstanceId())) {
                    results.add(result);
                }
            }
        }
        results.sort(Comparator.comparing(ValidationResult::taskId,
                Comparator.nullsLast(Comparator.naturalOrder())));
        return results;
    }

    /**
     * Returns per-task validation summaries for one process definition.
     */
    public List<ValidationSummary> summaryForProcess(String processId) {
        return summarize(result -> processId == null || processId.equals(result.processId()));
    }

    /**
     * Returns per-task validation summaries across every process.
     */
    public List<ValidationSummary> allSummaries() {
        return summarize(result -> true);
    }

    private List<ValidationSummary> summarize(java.util.function.Predicate<ValidationResult> filter) {
        Map<String, ValidationSummary> summaries = new LinkedHashMap<>();
        try (KeyValueIterator<String, ValidationResult> iterator = store().all()) {
            while (iterator.hasNext()) {
                ValidationResult result = iterator.next().value;
                if (result == null || !filter.test(result)) {
                    continue;
                }
                String key = result.processId() + ":" + result.taskId();
                summaries.merge(
                        key,
                        ValidationSummary.empty(result.processId(), result.taskId()).add(result),
                        (existing, added) -> existing.add(result));
            }
        }
        List<ValidationSummary> sorted = new ArrayList<>(summaries.values());
        sorted.sort(Comparator.comparing(ValidationSummary::processId, Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(ValidationSummary::taskId, Comparator.nullsLast(Comparator.naturalOrder())));
        return sorted;
    }

    /**
     * Looks up a single result by its composite key {@code processId:taskId:processInstanceId}.
     */
    public Optional<ValidationResult> findResult(String processId, String taskId, String processInstanceId) {
        return Optional.ofNullable(store().get(
                org.gautelis.durga.validation.ValidationCandidateOutput.key(processId, taskId, processInstanceId)));
    }

    private ValidationResult.MatchStatus parseStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        try {
            return ValidationResult.MatchStatus.valueOf(status.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private ReadOnlyKeyValueStore<String, ValidationResult> store() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        resultsStore,
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }
}
