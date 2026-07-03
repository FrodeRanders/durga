package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Query facade over the alarm state read model.
 */
public final class AlarmStateQueryService {
    private final KafkaStreams streams;
    private final String alarmStateStore;

    public AlarmStateQueryService(KafkaStreams streams, String alarmStateStore) {
        this.streams = streams;
        this.alarmStateStore = alarmStateStore;
    }

    public List<AlarmStateView> allAlarms() {
        List<AlarmStateView> results = new ArrayList<>();
        try (KeyValueIterator<String, AlarmStateView> iterator = store().all()) {
            while (iterator.hasNext()) {
                AlarmStateView state = iterator.next().value;
                if (state != null) {
                    results.add(state);
                }
            }
        }
        sort(results);
        return results;
    }

    public List<AlarmStateView> alarmsForProcess(String processId) {
        List<AlarmStateView> results = new ArrayList<>();
        try (KeyValueIterator<String, AlarmStateView> iterator = store().all()) {
            while (iterator.hasNext()) {
                AlarmStateView state = iterator.next().value;
                if (state != null && processId.equals(state.processId())) {
                    results.add(state);
                }
            }
        }
        sort(results);
        return results;
    }

    public List<AlarmStateView> alarmsForInstance(String processInstanceId) {
        List<AlarmStateView> results = new ArrayList<>();
        try (KeyValueIterator<String, AlarmStateView> iterator = store().all()) {
            while (iterator.hasNext()) {
                AlarmStateView state = iterator.next().value;
                if (state != null && processInstanceId.equals(state.lastProcessInstanceId())) {
                    results.add(state);
                }
            }
        }
        sort(results);
        return results;
    }

    private ReadOnlyKeyValueStore<String, AlarmStateView> store() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        alarmStateStore,
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private static void sort(List<AlarmStateView> results) {
        results.sort(Comparator
                .comparing(AlarmStateView::lastTriggeredAt, Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(AlarmStateView::key));
    }
}
