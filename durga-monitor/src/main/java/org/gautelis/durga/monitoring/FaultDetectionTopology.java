package org.gautelis.durga.monitoring;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
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
    static final String SEEN_STORE = "fault-seen-store";

    static final String CASCADE_ONSET_PREFIX = "$cascade-onsets:";
    static final String CASCADE_ALARM_PREFIX = "$cascade:";

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

        StoreBuilder<KeyValueStore<String, String>> seenStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SEEN_STORE),
                        Serdes.String(), Serdes.String());
        builder.addStateStore(seenStoreBuilder);

        // Stream 1: process events → evaluate alarms
        builder.stream(Pattern.compile(eventsPattern), Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new FaultDetectionProcessor(staticConfigs),
                        CONFIG_STORE, COUNTS_STORE, WINDOWS_STORE, LAST_ALARM_STORE, SEEN_STORE)
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
        private final AlarmConfig stuckConfig;
        private final AlarmConfig cascadeConfig;
        private final long startedAtMs;
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> configStore;
        private KeyValueStore<String, Integer> countsStore;
        private KeyValueStore<String, String> windowsStore;
        private KeyValueStore<String, Integer> lastAlarmStore;
        private KeyValueStore<String, String> seenStore;

        FaultDetectionProcessor(Set<AlarmConfig> staticConfigs) {
            this.staticConfigs = staticConfigs != null ? staticConfigs : Set.of();
            this.stuckConfig = this.staticConfigs.stream()
                    .filter(c -> c.syndrome() == AlarmSyndrome.STUCK && c.processId() == null)
                    .findFirst().orElse(null);
            this.cascadeConfig = this.staticConfigs.stream()
                    .filter(c -> c.syndrome() == AlarmSyndrome.CASCADE)
                    .findFirst().orElse(null);
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
            this.seenStore = context.getStateStore(SEEN_STORE);

            long scanMs = scanIntervalMs(stuckConfig, cascadeConfig);
            context.schedule(Duration.ofMillis(scanMs), PunctuationType.WALL_CLOCK_TIME, this::detectStalls);
            LOG.info("Stall/cascade detection scheduled every {}ms (builtin stuck={}, builtin cascade={})",
                    scanMs, stuckConfig != null, cascadeConfig != null);
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.timestamp() > 0 && record.timestamp() < startedAtMs) {
                return;
            }
            ProcessEvent event = parse(record.value());
            if (event == null) return;

            updateSeen(event);

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
                // STUCK / CASCADE are absence-of-progress syndromes handled by the punctuator.
                if (config.syndrome() == AlarmSyndrome.STUCK || config.syndrome() == AlarmSyndrome.CASCADE) {
                    continue;
                }
                if (event.eventType() != config.eventType()) continue;
                if (!config.matches(event.processId(), event.activityId())) continue;

                AlarmEvent alarm = switch (config.syndrome()) {
                    case HARD_ERROR -> newAlarm(config, event, now, 1);
                    case COUNTED -> evaluateCounted(config, event, now, countsStore, lastAlarmStore);
                    case SLIDING_WINDOW -> evaluateSlidingWindow(config, event, now, windowsStore, lastAlarmStore);
                    default -> null;
                };

                if (alarm != null) {
                    context.forward(record.withKey(alarm.alarmId()).withValue(alarm.toJson()));
                }
            }
        }

        /**
         * Records the latest lifecycle activity for an active instance, or drops it from tracking
         * once the instance reaches a terminal state. Every active instance is tracked so that a
         * per-process STUCK config (added via BPMN) can take effect even when the built-in default
         * is disabled.
         */
        private void updateSeen(ProcessEvent event) {
            String pid = event.processInstanceId();
            if (pid == null || pid.isBlank()) return;
            if (isTerminal(event.eventType())) {
                seenStore.delete(pid);
                return;
            }
            InstanceSeen seen = new InstanceSeen(
                    event.processId(),
                    event.activityId(),
                    event.eventType() != null ? event.eventType().name() : null,
                    System.currentTimeMillis(),
                    false);
            seenStore.put(pid, serializeSeen(seen));
        }

        /**
         * Wall-clock scan: for each active instance, resolves the applicable STUCK config
         * (explicit per-process / per-activity overriding the built-in default), emits a one-shot
         * STUCK alarm when idle beyond its timeout, and feeds every applicable cascade window
         * (system-wide default plus any per-process CASCADE configs).
         */
        private void detectStalls(long timestamp) {
            long nowMs = System.currentTimeMillis();
            String now = Instant.now().toString();

            List<KeyValue<String, String>> toFlag = new ArrayList<>();
            List<StuckHit> hits = new ArrayList<>();

            try (KeyValueIterator<String, String> it = seenStore.all()) {
                while (it.hasNext()) {
                    KeyValue<String, String> kv = it.next();
                    InstanceSeen seen = readSeen(kv.value);
                    if (seen == null || seen.stuckAlarmed()) continue;
                    AlarmConfig cfg = applicableStuckConfig(seen.processId(), seen.activityId());
                    if (cfg == null) continue;
                    long idleMs = nowMs - seen.lastSeenMs();
                    if (idleMs >= cfg.windowDuration().toMillis()) {
                        hits.add(new StuckHit(kv.key, seen, cfg, idleMs / 1000));
                        toFlag.add(new KeyValue<>(kv.key, serializeSeen(seen.markAlarmed())));
                    }
                }
            }

            for (KeyValue<String, String> kv : toFlag) {
                seenStore.put(kv.key, kv.value);
            }
            for (StuckHit hit : hits) {
                AlarmEvent stuck = newStuckAlarm(hit.config(), hit.seen().processId(), hit.instanceId(),
                        hit.seen().activityId(), parseType(hit.seen().lastEventType()), now, hit.idleSeconds());
                context.forward(new Record<>(stuck.alarmId(), stuck.toJson(), nowMs));

                for (AlarmConfig cascade : cascadeConfigsFor(hit.seen().processId(), hit.seen().activityId())) {
                    AlarmEvent evt = recordCascadeOnset(cascade, now, nowMs, windowsStore, lastAlarmStore);
                    if (evt != null) {
                        context.forward(new Record<>(evt.alarmId(), evt.toJson(), nowMs));
                    }
                }
            }

            for (AlarmConfig cascade : allCascadeConfigs()) {
                maintainCascade(cascade, nowMs, windowsStore, lastAlarmStore);
            }
        }

        /**
         * Resolves the STUCK config that governs an instance: an activity-specific config wins,
         * then a process-wide one, otherwise the built-in default (which may be null).
         */
        private AlarmConfig applicableStuckConfig(String processId, String activityId) {
            String stored = processId != null ? configStore.get(processId) : null;
            if (stored != null) {
                AlarmConfig processWide = null;
                for (AlarmConfig c : deserializeConfigs(stored)) {
                    if (c.syndrome() != AlarmSyndrome.STUCK) continue;
                    if (c.activityId() != null) {
                        if (c.activityId().equals(activityId)) return c;
                    } else if (c.matches(processId, activityId)) {
                        processWide = c;
                    }
                }
                if (processWide != null) return processWide;
            }
            return stuckConfig;
        }

        /** Cascade configs whose onset window a stall in this scope should feed. */
        private List<AlarmConfig> cascadeConfigsFor(String processId, String activityId) {
            List<AlarmConfig> result = new ArrayList<>();
            if (cascadeConfig != null) {
                result.add(cascadeConfig);
            }
            String stored = processId != null ? configStore.get(processId) : null;
            if (stored != null) {
                for (AlarmConfig c : deserializeConfigs(stored)) {
                    if (c.syndrome() == AlarmSyndrome.CASCADE && c.matches(processId, activityId)) {
                        result.add(c);
                    }
                }
            }
            return result;
        }

        /** Every cascade config known right now, for window maintenance/re-arming. */
        private Set<AlarmConfig> allCascadeConfigs() {
            Set<AlarmConfig> result = new HashSet<>();
            if (cascadeConfig != null) {
                result.add(cascadeConfig);
            }
            try (KeyValueIterator<String, String> it = configStore.all()) {
                while (it.hasNext()) {
                    for (AlarmConfig c : deserializeConfigs(it.next().value)) {
                        if (c.syndrome() == AlarmSyndrome.CASCADE) result.add(c);
                    }
                }
            }
            return result;
        }

        @Override
        public void close() {}
    }

    /** A resolved stall: the instance and the STUCK config that governs it. */
    record StuckHit(String instanceId, InstanceSeen seen, AlarmConfig config, long idleSeconds) {}

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
        String processId = event.processId() != null ? event.processId() : "unknown";
        String activityId = event.activityId() != null ? event.activityId() : "*";
        String processInstanceId = event.processInstanceId() != null ? event.processInstanceId() : "unknown";
        String template = config.message() != null ? config.message() : "";
        String msg = template
                .replace("${processId}", processId)
                .replace("${activityId}", activityId)
                .replace("${processInstanceId}", processInstanceId)
                .replace("${count}", String.valueOf(count));
        return new AlarmEvent(
                config.id() + "-" + UUID.randomUUID().toString().substring(0, 8),
                config.id(), config.syndrome(), config.severity(), msg,
                processId, processInstanceId, activityId,
                event.eventType(), now, count,
                config.syndrome() == AlarmSyndrome.HARD_ERROR ? 0 : config.threshold());
    }

    // ---- stuck / cascade evaluation (automatic layer) ----

    static String cascadeOnsetKey(AlarmConfig config) {
        return CASCADE_ONSET_PREFIX + config.id();
    }

    static String cascadeAlarmKey(AlarmConfig config) {
        return CASCADE_ALARM_PREFIX + config.id();
    }

    /**
     * Records a fresh stuck onset in the config's cascade window and returns a CASCADE alarm when
     * the number of onsets within {@code windowDuration} first exceeds the threshold; suppressed
     * until the window count drops back to or below the threshold.
     */
    static AlarmEvent recordCascadeOnset(AlarmConfig config, String now, long nowMs,
                                          KeyValueStore<String, String> windowsStore,
                                          KeyValueStore<String, Integer> lastAlarmStore) {
        String onsetKey = cascadeOnsetKey(config);
        String alarmKey = cascadeAlarmKey(config);
        long cutoffMs = nowMs - config.windowDuration().toMillis();
        List<Long> timestamps = parseTimestamps(windowsStore.get(onsetKey));
        timestamps.add(nowMs);
        timestamps.removeIf(ts -> ts < cutoffMs);
        windowsStore.put(onsetKey, serializeTimestamps(timestamps));

        int count = timestamps.size();
        Integer lastAlarmAt = lastAlarmStore.get(alarmKey);
        if (count > config.threshold() && lastAlarmAt == null) {
            lastAlarmStore.put(alarmKey, count);
            return newCascadeAlarm(config, now, count);
        }
        if (count <= config.threshold() && lastAlarmAt != null) {
            lastAlarmStore.delete(alarmKey);
        }
        return null;
    }

    /**
     * Prunes expired cascade onsets and re-arms the cascade alarm once the window count falls
     * back to or below the threshold, so a later surge can fire again.
     */
    static void maintainCascade(AlarmConfig config, long nowMs,
                                 KeyValueStore<String, String> windowsStore,
                                 KeyValueStore<String, Integer> lastAlarmStore) {
        String onsetKey = cascadeOnsetKey(config);
        String alarmKey = cascadeAlarmKey(config);
        long cutoffMs = nowMs - config.windowDuration().toMillis();
        List<Long> timestamps = parseTimestamps(windowsStore.get(onsetKey));
        boolean changed = timestamps.removeIf(ts -> ts < cutoffMs);
        if (changed) {
            windowsStore.put(onsetKey, serializeTimestamps(timestamps));
        }
        if (timestamps.size() <= config.threshold() && lastAlarmStore.get(alarmKey) != null) {
            lastAlarmStore.delete(alarmKey);
        }
    }

    static AlarmEvent newStuckAlarm(AlarmConfig config, String processId, String instanceId,
                                     String activityId, ProcessEvent.EventType lastEventType,
                                     String now, long idleSeconds) {
        String pid = processId != null ? processId : "*";
        String act = activityId != null ? activityId : "*";
        String msg = config.message()
                .replace("${processId}", pid)
                .replace("${activityId}", act)
                .replace("${processInstanceId}", instanceId != null ? instanceId : "*")
                .replace("${idleSeconds}", String.valueOf(idleSeconds))
                .replace("${count}", String.valueOf(idleSeconds));
        return new AlarmEvent(
                config.id() + "-" + UUID.randomUUID().toString().substring(0, 8),
                config.id(), AlarmSyndrome.STUCK, config.severity(), msg,
                pid, instanceId, activityId, lastEventType, now,
                (int) idleSeconds, (int) config.windowDuration().toSeconds());
    }

    static AlarmEvent newCascadeAlarm(AlarmConfig config, String now, int count) {
        String pid = config.processId() != null ? config.processId() : "*";
        String act = config.activityId() != null ? config.activityId() : "*";
        String msg = config.message()
                .replace("${count}", String.valueOf(count))
                .replace("${threshold}", String.valueOf(config.threshold()))
                .replace("${processId}", pid)
                .replace("${activityId}", act)
                .replace("${processInstanceId}", "*");
        return new AlarmEvent(
                config.id() + "-" + UUID.randomUUID().toString().substring(0, 8),
                config.id(), AlarmSyndrome.CASCADE, config.severity(), msg,
                pid, null, config.activityId(), null, now, count, config.threshold());
    }

    static boolean isTerminal(ProcessEvent.EventType type) {
        return type == ProcessEvent.EventType.PROCESS_COMPLETED
                || type == ProcessEvent.EventType.PROCESS_FAILED;
    }

    static ProcessEvent.EventType parseType(String name) {
        if (name == null) return null;
        try { return ProcessEvent.EventType.valueOf(name); } catch (IllegalArgumentException e) { return null; }
    }

    static long scanIntervalMs(AlarmConfig stuckConfig, AlarmConfig cascadeConfig) {
        String override = System.getProperty("durga.alarm.scan.interval.ms");
        if (override != null) {
            try { return Math.max(1000L, Long.parseLong(override)); } catch (NumberFormatException ignored) {}
        }
        long base = stuckConfig != null ? stuckConfig.windowDuration().toMillis()
                : cascadeConfig != null ? cascadeConfig.windowDuration().toMillis()
                : 60_000L;
        return Math.max(1000L, Math.min(base / 4, 30_000L));
    }

    static String serializeSeen(InstanceSeen seen) {
        try { return MAPPER.writeValueAsString(seen); } catch (Exception e) { return null; }
    }

    static InstanceSeen readSeen(String json) {
        if (json == null || json.isBlank()) return null;
        try { return MAPPER.readValue(json, InstanceSeen.class); } catch (Exception e) { return null; }
    }

    /**
     * Compact record of the last lifecycle activity observed for one active instance,
     * used by the stall detector.
     */
    record InstanceSeen(String processId, String activityId, String lastEventType,
                        long lastSeenMs, boolean stuckAlarmed) {
        InstanceSeen markAlarmed() {
            return new InstanceSeen(processId, activityId, lastEventType, lastSeenMs, true);
        }
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
        } catch (IOException ignored) {}
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
