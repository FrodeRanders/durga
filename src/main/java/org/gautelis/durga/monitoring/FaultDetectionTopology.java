package org.gautelis.durga.monitoring;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Kafka Streams topology for fault detection on process lifecycle events.
 * <p>
 * Subscribes to two streams:
 * <ul>
 *   <li>{@code process-events-.*} — evaluates each lifecycle event against
 *       active alarm configurations.</li>
 *   <li>{@code process-models} — processes BPMN model registrations to
 *       dynamically update alarm configurations extracted from Camunda
 *       extension properties.</li>
 * </ul>
 *
 * <h3>Alarm configuration sources</h3>
 * Configurations are merged from two sources:
 * <ol>
 *   <li><b>Programmatic</b> — a static set passed at topology build time.</li>
 *   <li><b>BPMN-derived</b> — extracted from {@code durga:alarm:<id>:<field>}
 *       Camunda properties on process and activity elements via
 *       {@link BpmnAlarmConfigParser}. Updated whenever a process model is
 *       published to {@code process-models}.</li>
 * </ol>
 *
 * @see BpmnAlarmConfigParser
 * @see AlarmConfig
 * @see AlarmSyndrome
 */
public final class FaultDetectionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FaultDetectionTopology.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<AlarmConfig>> CONFIG_LIST_TYPE = new TypeReference<>() {};

    static final String COUNTS_STORE = "fault-counts-store";
    static final String WINDOWS_STORE = "fault-windows-store";
    static final String LAST_ALARM_STORE = "fault-last-alarm-store";
    static final String CONFIG_STORE = "fault-config-store";

    public static final String DEFAULT_EVENTS_PATTERN = "process-events-.*";
    public static final String DEFAULT_ALARMS_TOPIC = "fault-alarms";
    public static final String DEFAULT_MODELS_TOPIC = "process-models";

    private FaultDetectionTopology() {}

    /**
     * Builds the fault detection topology.
     *
     * @param staticConfigs programmatic alarm configs (may be empty)
     * @param eventsPattern regex for lifecycle event topics
     * @param alarmsTopic   output topic for triggered alarms
     * @param modelsTopic   topic for BPMN model registrations (may be null
     *                      to disable dynamic configs)
     * @return the topology
     */
    public static Topology buildTopology(Set<AlarmConfig> staticConfigs, String eventsPattern,
                                          String alarmsTopic, String modelsTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, Integer>> countsStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(COUNTS_STORE),
                        Serdes.String(), Serdes.Integer());
        builder.addStateStore(countsStoreBuilder);

        StoreBuilder<KeyValueStore<String, String>> windowsStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(WINDOWS_STORE),
                        Serdes.String(), Serdes.String());
        builder.addStateStore(windowsStoreBuilder);

        StoreBuilder<KeyValueStore<String, Integer>> lastAlarmStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(LAST_ALARM_STORE),
                        Serdes.String(), Serdes.Integer());
        builder.addStateStore(lastAlarmStoreBuilder);

        StoreBuilder<KeyValueStore<String, String>> configStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(CONFIG_STORE),
                        Serdes.String(), Serdes.String());
        builder.addStateStore(configStoreBuilder);

        // Stream 1: process events → evaluate alarms
        builder.stream(Pattern.compile(eventsPattern), Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new FaultDetectionProcessor(staticConfigs),
                        CONFIG_STORE, COUNTS_STORE, WINDOWS_STORE, LAST_ALARM_STORE)
                .to(alarmsTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Stream 2: process models → update alarm configs
        if (modelsTopic != null && !modelsTopic.isBlank()) {
            builder.stream(modelsTopic, Consumed.with(Serdes.String(), Serdes.String()))
                    .process(ConfigUpdateProcessor::new, CONFIG_STORE);
        }

        return builder.build();
    }

    // ---- event processor ----

    static final class FaultDetectionProcessor implements Processor<String, String, String, String> {
        private final Set<AlarmConfig> staticConfigs;
        private final long startedAtMs;
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> configStore;
        private KeyValueStore<String, Integer> countsStore;
        private KeyValueStore<String, String> windowsStore;
        private KeyValueStore<String, Integer> lastAlarmStore;

        FaultDetectionProcessor(Set<AlarmConfig> staticConfigs) {
            this.staticConfigs = staticConfigs != null ? staticConfigs : Set.of();
            this.startedAtMs = System.currentTimeMillis();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.configStore = context.getStateStore(CONFIG_STORE);
            this.countsStore = context.getStateStore(COUNTS_STORE);
            this.windowsStore = context.getStateStore(WINDOWS_STORE);
            this.lastAlarmStore = context.getStateStore(LAST_ALARM_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.timestamp() > 0 && record.timestamp() < startedAtMs) {
                return;
            }
            ProcessEvent event = parse(record.value());
            if (event == null) return;

            String now = Instant.now().toString();
            Set<AlarmConfig> allConfigs = new HashSet<>(staticConfigs);

            // merge BPMN-derived configs for this process
            String stored = configStore.get(event.processId());
            if (stored != null) {
                for (AlarmConfig c : deserializeConfigs(stored)) {
                    allConfigs.add(c);
                }
            }

            for (AlarmConfig config : allConfigs) {
                if (event.eventType() != config.eventType()) continue;
                if (!config.matches(event.processId(), event.activityId())) continue;

                AlarmEvent alarm = switch (config.syndrome()) {
                    case HARD_ERROR -> newAlarm(config, event, now, 1);
                    case COUNTED -> evaluateCounted(config, event, now, countsStore, lastAlarmStore);
                    case SLIDING_WINDOW -> evaluateSlidingWindow(config, event, now, windowsStore, lastAlarmStore);
                };

                if (alarm != null) {
                    context.forward(record.withKey(alarm.alarmId()).withValue(alarm.toJson()));
                }
            }
        }

        @Override
        public void close() {}
    }

    // ---- model processor ----

    static final class ConfigUpdateProcessor implements Processor<String, String, String, String> {
        private KeyValueStore<String, String> configStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext<String, String> context) {
            this.configStore = context.getStateStore(CONFIG_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            String processId = record.key();
            String bpmnXml = record.value();
            if (processId == null || bpmnXml == null || bpmnXml.isBlank()) return;

            List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmnXml);
            if (configs.isEmpty()) {
                configStore.delete(processId);
            } else {
                configStore.put(processId, serializeConfigs(configs));
                LOG.info("Updated {} alarm config(s) for process '{}'", configs.size(), processId);
            }
        }

        @Override
        public void close() {}
    }

    // ---- package-private evaluation (for unit testing) ----

    static AlarmEvent evaluateCounted(AlarmConfig config, ProcessEvent event, String now,
                                       KeyValueStore<String, Integer> countsStore,
                                       KeyValueStore<String, Integer> lastAlarmStore) {
        String key = storeKey(config, event);
        Integer current = countsStore.get(key);
        int count = (current != null ? current : 0) + 1;
        countsStore.put(key, count);

        Integer lastAlarmAt = lastAlarmStore.get(key);
        if (count > config.threshold() && lastAlarmAt == null) {
            lastAlarmStore.put(key, count);
            return newAlarm(config, event, now, count);
        }
        if (count <= config.threshold() && lastAlarmAt != null) {
            lastAlarmStore.delete(key);
        }
        return null;
    }

    static AlarmEvent evaluateSlidingWindow(AlarmConfig config, ProcessEvent event, String now,
                                             KeyValueStore<String, String> windowsStore,
                                             KeyValueStore<String, Integer> lastAlarmStore) {
        String key = storeKey(config, event);
        long windowMs = config.windowDuration().toMillis();
        long currentMs = System.currentTimeMillis();
        long cutoffMs = currentMs - windowMs;

        String raw = windowsStore.get(key);
        List<Long> timestamps = parseTimestamps(raw);
        timestamps.add(currentMs);
        timestamps.removeIf(ts -> ts < cutoffMs);
        windowsStore.put(key, serializeTimestamps(timestamps));

        int count = timestamps.size();
        Integer lastAlarmAt = lastAlarmStore.get(key);
        if (count > config.threshold() && lastAlarmAt == null) {
            lastAlarmStore.put(key, count);
            return newAlarm(config, event, now, count);
        }
        if (count <= config.threshold() && lastAlarmAt != null) {
            lastAlarmStore.delete(key);
        }
        return null;
    }

    static String storeKey(AlarmConfig config, ProcessEvent event) {
        return config.id() + ":" + event.processId() + ":" +
                (event.activityId() != null ? event.activityId() : "*");
    }

    static AlarmEvent newAlarm(AlarmConfig config, ProcessEvent event, String now, int count) {
        String msg = config.message()
                .replace("${processId}", event.processId())
                .replace("${activityId}", event.activityId() != null ? event.activityId() : "*")
                .replace("${processInstanceId}", event.processInstanceId())
                .replace("${count}", String.valueOf(count));
        return new AlarmEvent(
                config.id() + "-" + UUID.randomUUID().toString().substring(0, 8),
                config.id(), config.syndrome(), config.severity(), msg,
                event.processId(), event.processInstanceId(), event.activityId(),
                event.eventType(), now, count,
                config.syndrome() == AlarmSyndrome.HARD_ERROR ? 0 : config.threshold());
    }

    static ProcessEvent parse(String json) {
        try { return ProcessEvent.fromJson(json); } catch (Exception e) {
            LOG.debug("Failed to parse event for fault detection: {}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    static List<Long> parseTimestamps(String raw) {
        List<Long> list = new ArrayList<>();
        if (raw == null || raw.isBlank()) return list;
        try {
            for (Object item : MAPPER.readValue(raw, List.class)) {
                if (item instanceof Number n) list.add(n.longValue());
            }
        } catch (Exception ignored) {}
        return list;
    }

    static String serializeTimestamps(List<Long> timestamps) {
        try { return MAPPER.writeValueAsString(timestamps); } catch (Exception e) { return "[]"; }
    }

    static String serializeConfigs(List<AlarmConfig> configs) {
        try { return MAPPER.writeValueAsString(configs); } catch (Exception e) { return "[]"; }
    }

    static List<AlarmConfig> deserializeConfigs(String json) {
        if (json == null || json.isBlank()) return List.of();
        try { return MAPPER.readValue(json, CONFIG_LIST_TYPE); } catch (Exception e) {
            LOG.debug("Failed to deserialize alarm configs: {}", e.getMessage());
            return List.of();
        }
    }
}
