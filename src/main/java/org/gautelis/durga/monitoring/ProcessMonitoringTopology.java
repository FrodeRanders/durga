package org.gautelis.durga.monitoring;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.gautelis.durga.ProcessEvent;

/**
 * Builds the Kafka Streams read model used by the monitoring HTTP API, CLI, and dashboard.
 * <p>
 * The topology materializes the latest state per instance and counts grouped by current state.
 */
public final class ProcessMonitoringTopology {
    public static final String DEFAULT_EVENTS_TOPIC = "process-events";
    public static final String DEFAULT_STATE_TOPIC = "process-state";
    public static final String DEFAULT_COUNTS_TOPIC = "process-state-counts";
    public static final String DEFAULT_STATE_STORE = "process-state-store";
    public static final String DEFAULT_COUNTS_STORE = "process-state-counts-store";

    private ProcessMonitoringTopology() {
    }

    /**
     * Builds the monitoring topology for the supplied topic and store names.
     *
     * @param topics logical topic and state-store names
     * @return topology that reads lifecycle events and materializes monitoring projections
     */
    public static Topology buildTopology(MonitoringTopics topics) {
        StreamsBuilder builder = new StreamsBuilder();
        var processEventSerde = JsonSerde.forClass(ProcessEvent.class);
        var processStateSerde = JsonSerde.forClass(ProcessStateView.class);
        var processStateCountSerde = JsonSerde.forClass(ProcessStateCount.class);

        KStream<String, ProcessEvent> events = builder.stream(
                topics.eventsTopic(),
                Consumed.with(Serdes.String(), processEventSerde)
        ).filter((key, event) -> key != null && event != null);

        KTable<String, ProcessStateView> stateByInstance = events
                .groupByKey(Grouped.with(Serdes.String(), processEventSerde))
                .aggregate(
                        ProcessStateView::empty,
                        (processInstanceId, event, currentState) -> currentState.apply(event),
                        Materialized.<String, ProcessStateView, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(topics.stateStore())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(processStateSerde)
                );

        stateByInstance.toStream()
                .to(topics.stateTopic(), Produced.with(Serdes.String(), processStateSerde));

        KTable<String, Long> countsByState = stateByInstance
                .filter((processInstanceId, state) -> state != null)
                .groupBy(
                        (processInstanceId, state) -> KeyValue.pair(state.currentStateKey(), state),
                        Grouped.with(Serdes.String(), processStateSerde)
                )
                .count(
                        Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(topics.countsStore())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        countsByState.toStream()
                .mapValues(ProcessStateCount::fromStateKey)
                .to(topics.countsTopic(), Produced.with(Serdes.String(), processStateCountSerde));

        return builder.build();
    }

    /**
     * Names of the monitoring topics and queryable state stores.
     *
     * @param eventsTopic canonical lifecycle-event topic
     * @param stateTopic latest-state output topic
     * @param countsTopic state-count output topic
     * @param stateStore queryable store for latest instance state
     * @param countsStore queryable store for counts by state
     */
    public record MonitoringTopics(
            String eventsTopic,
            String stateTopic,
            String countsTopic,
            String stateStore,
            String countsStore
    ) {
        /**
         * Returns the default monitoring topic and store names used throughout the repo.
         *
         * @return default topic and store names
         */
        public static MonitoringTopics defaults() {
            return new MonitoringTopics(
                    DEFAULT_EVENTS_TOPIC,
                    DEFAULT_STATE_TOPIC,
                    DEFAULT_COUNTS_TOPIC,
                    DEFAULT_STATE_STORE,
                    DEFAULT_COUNTS_STORE
            );
        }
    }
}
