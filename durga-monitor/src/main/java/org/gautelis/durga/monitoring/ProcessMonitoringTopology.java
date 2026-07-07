package org.gautelis.durga.monitoring;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

/**
 * Builds the Kafka Streams read model used by the monitoring HTTP API, CLI, and dashboard.
 * <p>
 * The topology materializes the latest state per instance, counts grouped by current state,
 * latency summaries, an active-instance index, and coarse history/trend buckets.
 */
public final class ProcessMonitoringTopology {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitoringTopology.class);

    public static final String DEFAULT_EVENTS_TOPIC = "process-events-default";
    public static final String DEFAULT_STATE_TOPIC = "process-state";
    public static final String DEFAULT_COUNTS_TOPIC = "process-state-counts";
    public static final String DEFAULT_ACTIVE_TOPIC = "process-active-state";
    public static final String DEFAULT_LATENCY_TOPIC = "process-latency";
    public static final String DEFAULT_TRENDS_TOPIC = "process-trends";
    public static final String DEFAULT_THROUGHPUT_TOPIC = "process-activity-throughput";
    public static final String DEFAULT_STATE_STORE = "process-state-global-store";
    public static final String DEFAULT_COUNTS_STORE = "process-state-counts-global-store";
    public static final String DEFAULT_ACTIVE_STORE = "process-active-state-global-store";
    public static final String DEFAULT_LATENCY_STORE = "process-latency-global-store";
    public static final String DEFAULT_TRENDS_STORE = "process-trends-global-store";
    public static final String DEFAULT_THROUGHPUT_STORE = "process-activity-throughput-global-store";
    public static final String DEFAULT_MODELS_TOPIC = "process-models";
    public static final String DEFAULT_MODELS_STORE = "process-models-store";

    private static final String LOCAL_STATE_STORE = "process-state-store";
    private static final String LOCAL_COUNTS_STORE = "process-state-counts-store";
    private static final String LOCAL_LATENCY_STORE = "process-latency-store";
    private static final String LOCAL_ACTIVITY_ENTRY_STORE = "process-activity-entry-store";
    private static final String LOCAL_TRENDS_STORE = "process-trends-store";
    private static final String LOCAL_THROUGHPUT_STORE = "process-activity-throughput-store";

    private static final long SLA_THRESHOLD_MS = slaThresholdMs();

    private static long slaThresholdMs() {
        String raw = System.getProperty("durga.sla.threshold.ms", "0");
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid durga.sla.threshold.ms '{}', defaulting to 0", raw);
            return 0L;
        }
    }

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
        var latencyDeltaSerde = JsonSerde.forClass(ActivityLatencyDelta.class);
        var latencyStatsSerde = JsonSerde.forClass(ActivityLatencyStats.class);
        var latencySummarySerde = JsonSerde.forClass(ActivityLatencySummary.class);
        var trendSerde = JsonSerde.forClass(ProcessTrendPoint.class);
        var throughputSerde = JsonSerde.forClass(ActivityThroughput.class);

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LOCAL_ACTIVITY_ENTRY_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );

        KStream<String, ProcessEvent> events;
        if (topics.eventsPattern() != null) {
            events = builder.stream(topics.eventsPattern(),
                    Consumed.with(Serdes.String(), processEventSerde));
        } else {
            events = builder.stream(topics.eventsTopic(),
                    Consumed.with(Serdes.String(), processEventSerde));
        }
        events = events
                .filter((key, event) -> event != null)
                .selectKey((key, event) -> key != null && !key.isBlank()
                        ? key : event.processInstanceId())
                .filter((key, event) -> key != null && !key.isBlank());

        KTable<String, ProcessStateView> stateByInstance = events
                .groupByKey(Grouped.with(Serdes.String(), processEventSerde))
                .aggregate(
                        ProcessStateView::empty,
                        (processInstanceId, event, currentState) -> currentState.apply(event),
                        Materialized.<String, ProcessStateView, KeyValueStore<Bytes, byte[]>>as(LOCAL_STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(processStateSerde)
                );

        stateByInstance.toStream()
                .to(topics.stateTopic(), Produced.with(Serdes.String(), processStateSerde));

        stateByInstance.toStream()
                .mapValues(state -> state != null && "active".equals(state.lifecycleState()) ? state : null)
                .to(topics.activeTopic(), Produced.with(Serdes.String(), processStateSerde));

        KTable<String, Long> countsByState = stateByInstance
                .filter((processInstanceId, state) -> state != null)
                .groupBy(
                        (processInstanceId, state) -> KeyValue.pair(state.currentStateKey(), state),
                        Grouped.with(Serdes.String(), processStateSerde)
                )
                .count(
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(LOCAL_COUNTS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        countsByState.toStream()
                .mapValues(ProcessStateCount::fromStateKey)
                .to(topics.countsTopic(), Produced.with(Serdes.String(), processStateCountSerde));

        KStream<String, ActivityLatencyDelta> latencySamples = events
                .process(ActivityLatencyProcessor::new, LOCAL_ACTIVITY_ENTRY_STORE)
                .filter((key, value) -> key != null && value != null);

        KTable<String, ActivityLatencyStats> latencyStats = latencySamples
                .groupByKey(Grouped.with(Serdes.String(), latencyDeltaSerde))
                .aggregate(
                        ActivityLatencyStats::empty,
                        (latencyKey, sample, current) -> current.add(sample),
                        Materialized.<String, ActivityLatencyStats, KeyValueStore<Bytes, byte[]>>as(LOCAL_LATENCY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(latencyStatsSerde)
                );

        latencyStats.toStream()
                .mapValues(ActivityLatencyStats::toSummary)
                .to(topics.latencyTopic(), Produced.with(Serdes.String(), latencySummarySerde));

        KTable<String, Long> trendCounts = events
                .filter((processInstanceId, event) -> ProcessTrendPoint.supportedEventType(event.eventType()))
                .map((processInstanceId, event) -> KeyValue.pair(ProcessTrendPoint.keyFor(event), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .count(
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(LOCAL_TRENDS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        trendCounts.toStream()
                .mapValues(ProcessTrendPoint::fromKey)
                .to(topics.trendsTopic(), Produced.with(Serdes.String(), trendSerde));

        // Per-activity throughput: count terminal per-activity events so plugin/data-pipeline tasks
        // (which emit only ACTIVITY_COMPLETED, never ACTIVITY_ENTERED) still report how many items
        // they have processed, independent of the latency pairing.
        KTable<String, ActivityThroughput> throughput = events
                .filter((processInstanceId, event) -> event.activityId() != null
                        && !event.activityId().isBlank()
                        && isThroughputEvent(event.eventType()))
                .map((processInstanceId, event) ->
                        KeyValue.pair(event.processId() + ":" + event.activityId(), event))
                .groupByKey(Grouped.with(Serdes.String(), processEventSerde))
                .aggregate(
                        ActivityThroughput::empty,
                        (key, event, current) -> current.increment(event.processId(), event.activityId()),
                        Materialized.<String, ActivityThroughput, KeyValueStore<Bytes, byte[]>>as(LOCAL_THROUGHPUT_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(throughputSerde)
                );

        throughput.toStream()
                .to(topics.throughputTopic(), Produced.with(Serdes.String(), throughputSerde));

        builder.globalTable(
                topics.stateTopic(),
                Consumed.with(Serdes.String(), processStateSerde),
                Materialized.<String, ProcessStateView, KeyValueStore<Bytes, byte[]>>as(topics.stateStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(processStateSerde)
        );

        builder.globalTable(
                topics.countsTopic(),
                Consumed.with(Serdes.String(), processStateCountSerde),
                Materialized.<String, ProcessStateCount, KeyValueStore<Bytes, byte[]>>as(topics.countsStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(processStateCountSerde)
        );

        builder.globalTable(
                topics.activeTopic(),
                Consumed.with(Serdes.String(), processStateSerde),
                Materialized.<String, ProcessStateView, KeyValueStore<Bytes, byte[]>>as(topics.activeStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(processStateSerde)
        );

        builder.globalTable(
                topics.latencyTopic(),
                Consumed.with(Serdes.String(), latencySummarySerde),
                Materialized.<String, ActivityLatencySummary, KeyValueStore<Bytes, byte[]>>as(topics.latencyStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(latencySummarySerde)
        );

        builder.globalTable(
                topics.trendsTopic(),
                Consumed.with(Serdes.String(), trendSerde),
                Materialized.<String, ProcessTrendPoint, KeyValueStore<Bytes, byte[]>>as(topics.trendsStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(trendSerde)
        );

        builder.globalTable(
                topics.throughputTopic(),
                Consumed.with(Serdes.String(), throughputSerde),
                Materialized.<String, ActivityThroughput, KeyValueStore<Bytes, byte[]>>as(topics.throughputStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(throughputSerde)
        );

        // Process BPMN model cache: processes post their models keyed by processId
        builder.globalTable(
                topics.modelsTopic(),
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(topics.modelsStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        return builder.build();
    }

    /**
     * Names of the monitoring topics and queryable state stores.
     */
    public record MonitoringTopics(
            String eventsTopic,
            String stateTopic,
            String countsTopic,
            String activeTopic,
            String latencyTopic,
            String trendsTopic,
            String throughputTopic,
            String modelsTopic,
            String stateStore,
            String countsStore,
            String activeStore,
            String latencyStore,
            String trendsStore,
            String throughputStore,
            String modelsStore,
            Pattern eventsPattern,
            boolean multiProcess
    ) {
        /**
         * Matches all {@code process-events-*} topics except the validation events topics
         * ({@code process-events-*-validation}), so the production monitor is not polluted
         * by shadow-process lifecycle events.
         */
        private static final Pattern ALL_EVENTS_PATTERN = Pattern.compile("process-events-(?!.*-validation).*");

        /**
         * Returns topic and store names for monitoring ALL processes in a single instance.
         * Subscribes to all {@code process-events-*} topics via regex.
         */
        public static MonitoringTopics forAllProcesses() {
            return new MonitoringTopics(
                    DEFAULT_EVENTS_TOPIC,
                    DEFAULT_STATE_TOPIC,
                    DEFAULT_COUNTS_TOPIC,
                    DEFAULT_ACTIVE_TOPIC,
                    DEFAULT_LATENCY_TOPIC,
                    DEFAULT_TRENDS_TOPIC,
                    DEFAULT_THROUGHPUT_TOPIC,
                    DEFAULT_MODELS_TOPIC,
                    DEFAULT_STATE_STORE,
                    DEFAULT_COUNTS_STORE,
                    DEFAULT_ACTIVE_STORE,
                    DEFAULT_LATENCY_STORE,
                    DEFAULT_TRENDS_STORE,
                    DEFAULT_THROUGHPUT_STORE,
                    DEFAULT_MODELS_STORE,
                    ALL_EVENTS_PATTERN,
                    true
            );
        }
    }

    /**
     * Whether an event type represents an item flowing out of an activity, and so counts toward that
     * activity's throughput.
     */
    private static boolean isThroughputEvent(ProcessEvent.EventType eventType) {
        return eventType == ProcessEvent.EventType.ACTIVITY_COMPLETED
                || eventType == ProcessEvent.EventType.ACTIVITY_ESCALATED
                || eventType == ProcessEvent.EventType.GATEWAY_TAKEN;
    }

    private static final class ActivityLatencyProcessor implements Processor<String, ProcessEvent, String, ActivityLatencyDelta> {
        private KeyValueStore<String, String> entryStore;
        private ProcessorContext<String, ActivityLatencyDelta> context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext<String, ActivityLatencyDelta> context) {
            this.context = context;
            this.entryStore = context.getStateStore(LOCAL_ACTIVITY_ENTRY_STORE);
        }

        @Override
        public void process(Record<String, ProcessEvent> record) {
            String processInstanceId = record.key();
            ProcessEvent event = record.value();
            if (event == null || event.activityId() == null || event.activityId().isBlank()) {
                return;
            }

            String key = processInstanceId + ":" + event.activityId();
            if (event.eventType() == ProcessEvent.EventType.ACTIVITY_ENTERED) {
                entryStore.put(key, event.timestamp());
                return;
            }

            if (!supportsLatency(event.eventType())) {
                return;
            }

            String enteredAt = entryStore.get(key);
            if (enteredAt == null || enteredAt.isBlank()) {
                return;
            }
            Instant entered = Timestamps.parseOrNull(enteredAt);
            Instant completed = Timestamps.parseOrNull(event.timestamp());
            if (entered == null || completed == null) {
                // Malformed timestamps: drop the entry so it does not accumulate, and skip.
                entryStore.delete(key);
                return;
            }
            entryStore.delete(key);
            long durationMs = Math.max(0L, ChronoUnit.MILLIS.between(entered, completed));
            ActivityLatencyDelta sample = ActivityLatencyDelta.of(
                    event.processId(), event.activityId(), durationMs, SLA_THRESHOLD_MS);
            context.forward(record.withKey(sample.key()).withValue(sample));
        }

        private boolean supportsLatency(ProcessEvent.EventType eventType) {
            return eventType == ProcessEvent.EventType.ACTIVITY_COMPLETED
                    || eventType == ProcessEvent.EventType.ACTIVITY_ESCALATED
                    || eventType == ProcessEvent.EventType.ACTIVITY_CANCELLED
                    || eventType == ProcessEvent.EventType.PROCESS_FAILED;
        }
    }
}
