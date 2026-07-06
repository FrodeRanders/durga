package org.gautelis.durga.monitoring;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.gautelis.durga.KafkaIntegrationTestBase;
import org.gautelis.durga.ProcessEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end verification that a performance-SLA breach against a live Kafka broker
 * produces an SLA alarm and materializes it into the alarm-state read model that
 * {@code /api/metrics} exposes.
 *
 * <p>Runs both the fault-detection topology (which measures SLAs from lifecycle events
 * and emits alarms to {@code fault-alarms}) and the alarm-state topology (which folds
 * those alarms into the queryable read model).
 */
public class SlaMonitoringIntegrationTest extends KafkaIntegrationTestBase {

    private static final String SUFFIX = "-sla-" + UUID.randomUUID().toString().substring(0, 8);
    private static final Duration DEFAULT_TIMEOUT = resolveTimeout();

    private static final String EVENTS_TOPIC = "process-events" + SUFFIX;
    private static final String EVENTS_PATTERN = "process-events" + SUFFIX;
    private static final String ALARMS_TOPIC = "fault-alarms" + SUFFIX;
    private static final String ALARM_STATE_STORE = "alarm-state-store" + SUFFIX;

    private KafkaProducer<String, String> producer;
    private KafkaStreams faultStreams;
    private KafkaStreams alarmStateStreams;
    private Path faultStateDir;
    private Path alarmStateDir;
    private AlarmStateQueryService alarmQuery;

    @BeforeClass
    public static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(
                    new NewTopic(EVENTS_TOPIC, 1, (short) 1),
                    new NewTopic(ALARMS_TOPIC, 1, (short) 1)
            )).all().get();
        }
    }

    @Before
    public void setUp() throws IOException {
        // Fast punctuator so throughput SLAs are scored promptly in the test.
        System.setProperty("durga.alarm.scan.interval.ms", "1000");

        faultStateDir = Files.createTempDirectory("kafka-streams-sla-fault-");
        alarmStateDir = Files.createTempDirectory("kafka-streams-sla-alarm-");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Two SLAs declared up front: a per-task latency ceiling and a process-wide minimum rate.
        AlarmConfig latencySla = new AlarmConfig(
                "sla_proc:validate-latency", "sla_proc", "validate_order",
                null, AlarmSyndrome.SLA_LATENCY, 0, Duration.ofMillis(1000),
                AlarmSeverity.WARN, "validate_order took ${latencyMs}ms > ${limitMs}ms",
                AlarmOrigin.EXPLICIT);
        AlarmConfig throughputSla = new AlarmConfig(
                "rate_proc:min-rate", "rate_proc", null,
                null, AlarmSyndrome.SLA_THROUGHPUT, 2, Duration.ofSeconds(2),
                AlarmSeverity.CRITICAL, "throughput ${count}/${windowSeconds}s below ${threshold}",
                AlarmOrigin.EXPLICIT);
        Set<AlarmConfig> configs = Set.of(latencySla, throughputSla);

        Topology faultTopology = FaultDetectionTopology.buildTopology(
                configs, EVENTS_PATTERN, ALARMS_TOPIC, null);
        faultStreams = new KafkaStreams(faultTopology, streamsProps("sla-fault", faultStateDir));
        faultStreams.start();
        waitForCondition(() -> faultStreams.state() == KafkaStreams.State.RUNNING, DEFAULT_TIMEOUT);

        Topology alarmStateTopology = AlarmStateTopology.buildTopology(ALARMS_TOPIC, ALARM_STATE_STORE);
        alarmStateStreams = new KafkaStreams(alarmStateTopology, streamsProps("sla-alarm", alarmStateDir));
        alarmStateStreams.start();
        waitForCondition(() -> alarmStateStreams.state() == KafkaStreams.State.RUNNING, DEFAULT_TIMEOUT);

        alarmQuery = new AlarmStateQueryService(alarmStateStreams, ALARM_STATE_STORE);
    }

    @After
    public void tearDown() {
        System.clearProperty("durga.alarm.scan.interval.ms");
        closeStreams(faultStreams);
        closeStreams(alarmStateStreams);
        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) { }
        }
        deleteQuietly(faultStateDir);
        deleteQuietly(alarmStateDir);
    }

    @Test
    public void shouldFireSlaLatencyAlarmWhenTaskExceedsLimit() {
        System.out.println("TC: a slow task breaches its SLA_LATENCY ceiling and the alarm reaches the read model");

        String instanceId = "pi-sla-1";
        // entered at 08:00:00, completed at 08:04:00 -> 240000ms measured latency, limit is 1000ms
        produceEvent(instanceId, "sla_proc", "validate_order",
                ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:00:00Z");
        produceEvent(instanceId, "sla_proc", "validate_order",
                ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-04-03T08:04:00Z");

        waitForCondition(() -> slaAlarm(AlarmSyndrome.SLA_LATENCY).isPresent(), DEFAULT_TIMEOUT);

        AlarmStateView alarm = slaAlarm(AlarmSyndrome.SLA_LATENCY).orElseThrow();
        assertEquals("sla_proc", alarm.processId());
        assertEquals("validate_order", alarm.activityId());
        assertEquals(AlarmSeverity.WARN, alarm.severity());
        // These are exactly the values /api/metrics emits as durga_sla_last_observed / durga_sla_limit.
        assertEquals(240000, alarm.lastCount());
        assertEquals(1000, alarm.threshold());
        assertTrue(alarm.lastMessage().contains("240000ms"));
    }

    @Test
    public void shouldFireSlaThroughputAlarmWhenRateStarves() {
        System.out.println("TC: a starved pipeline breaches its SLA_THROUGHPUT floor and the punctuator alarms");

        // Feed a single completion; the SLA requires >= 2 per 2s window, so after a full
        // window elapses with nothing more arriving the punctuator reports a breach.
        produceEvent("pi-rate-1", "rate_proc", null,
                ProcessEvent.EventType.PROCESS_COMPLETED, "2026-04-03T08:00:00Z");

        waitForCondition(() -> slaAlarm(AlarmSyndrome.SLA_THROUGHPUT).isPresent(), DEFAULT_TIMEOUT);

        AlarmStateView alarm = slaAlarm(AlarmSyndrome.SLA_THROUGHPUT).orElseThrow();
        assertEquals("rate_proc", alarm.processId());
        assertEquals(AlarmSyndrome.SLA_THROUGHPUT, alarm.syndrome());
        assertEquals(AlarmSeverity.CRITICAL, alarm.severity());
        // observed rate below the required minimum (threshold)
        assertTrue(alarm.lastCount() < alarm.threshold());
        assertEquals(2, alarm.threshold());
    }

    private Optional<AlarmStateView> slaAlarm(AlarmSyndrome syndrome) {
        try {
            return alarmQuery.allAlarms().stream()
                    .filter(a -> a.syndrome() == syndrome)
                    .findFirst();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private void produceEvent(String instanceId, String processId, String activityId,
                               ProcessEvent.EventType eventType, String timestamp) {
        ProcessEvent.Status status = switch (eventType) {
            case ACTIVITY_COMPLETED, PROCESS_COMPLETED -> ProcessEvent.Status.COMPLETED;
            default -> ProcessEvent.Status.STARTED;
        };
        ProcessEvent event = new ProcessEvent(
                instanceId, processId, activityId, "token", "corr",
                Map.of(), status, null, eventType, "v1", "BK", timestamp);
        producer.send(new ProducerRecord<>(EVENTS_TOPIC, instanceId, event.toJson()));
        producer.flush();
    }

    private Properties streamsProps(String app, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, app + "-" + UUID.randomUUID().toString().substring(0, 8));
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            try { streams.close(Duration.ofSeconds(10)); } catch (Exception ignored) { }
            try { streams.cleanUp(); } catch (Exception ignored) { }
        }
    }

    private static void deleteQuietly(Path path) {
        if (path == null) return;
        try { deleteRecursive(path); } catch (IOException ignored) { }
    }

    static void waitForCondition(Callable<Boolean> condition, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.call()) {
                    return;
                }
            } catch (Exception ignored) {
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        fail("Condition not met within timeout: " + timeout);
    }

    private static void deleteRecursive(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                entries.forEach(p -> {
                    try { deleteRecursive(p); } catch (IOException ignored) { }
                });
            }
        }
        Files.deleteIfExists(path);
    }

    private static Duration resolveTimeout() {
        String val = System.getProperty("durga.it.timeout.seconds",
                System.getenv().getOrDefault("DURGA_IT_TIMEOUT_SECONDS", "60"));
        try {
            return Duration.ofSeconds(Long.parseLong(val));
        } catch (NumberFormatException e) {
            return Duration.ofSeconds(60);
        }
    }
}
