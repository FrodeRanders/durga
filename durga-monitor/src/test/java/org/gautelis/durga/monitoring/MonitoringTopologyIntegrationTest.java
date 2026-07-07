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
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class MonitoringTopologyIntegrationTest extends KafkaIntegrationTestBase {
    private static final String SUFFIX = "-mt-" + UUID.randomUUID().toString().substring(0, 8);
    private static final Duration DEFAULT_TIMEOUT = resolveTimeout();
    private static final ProcessMonitoringTopology.MonitoringTopics TOPICS = new ProcessMonitoringTopology.MonitoringTopics(
            "process-events" + SUFFIX,
            "process-state" + SUFFIX,
            "process-state-counts" + SUFFIX,
            "process-active-state" + SUFFIX,
            "process-latency" + SUFFIX,
            "process-trends" + SUFFIX,
            "process-activity-throughput" + SUFFIX,
            "process-models" + SUFFIX,
            "process-state-global-store" + SUFFIX,
            "process-state-counts-global-store" + SUFFIX,
            "process-active-state-global-store" + SUFFIX,
            "process-latency-global-store" + SUFFIX,
            "process-trends-global-store" + SUFFIX,
            "process-activity-throughput-global-store" + SUFFIX,
            "process-models-store" + SUFFIX,
            null,
            false
    );

    private KafkaProducer<String, String> producer;
    private KafkaStreams streams;
    private Path stateDir;
    private ProcessMonitoringQueryService queryService;

    @BeforeClass
    public static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(TOPICS.eventsTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.stateTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.countsTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.activeTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.latencyTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.trendsTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.throughputTopic(), 1, (short) 1),
                    new NewTopic(TOPICS.modelsTopic(), 1, (short) 1)
            );
            admin.createTopics(topics).all().get();
        }
    }

    @Before
    public void setUp() throws IOException {
        stateDir = Files.createTempDirectory("kafka-streams-mt-");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "monitoring-mt-" + UUID.randomUUID().toString().substring(0, 8));
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = ProcessMonitoringTopology.buildTopology(TOPICS);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();
        waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, DEFAULT_TIMEOUT);

        queryService = new ProcessMonitoringQueryService(streams, TOPICS);
    }

    @After
    public void tearDown() {
        if (streams != null) {
            try { streams.close(Duration.ofSeconds(10)); } catch (Exception ignored) {}
            try { streams.cleanUp(); } catch (Exception ignored) {}
        }
        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) {}
        }
        if (stateDir != null) {
            try { deleteRecursive(stateDir); } catch (IOException ignored) {}
        }
    }

    @Test
    public void shouldMaterializeProcessStateFromLifecycleEvents() {
        System.out.println("TC: materializes process state from lifecycle events and verifies state store");

        String instanceId = "pi-mt-1";
        produceEvent(instanceId, "test_proc", "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:00:00Z");
        produceEvent(instanceId, "test_proc", "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:01:00Z");
        produceEvent(instanceId, "test_proc", "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-04-03T08:05:00Z");

        waitForCondition(() -> queryService.findInstance(instanceId).isPresent(), DEFAULT_TIMEOUT);

        Optional<ProcessStateView> stateOpt = queryService.findInstance(instanceId);
        assertTrue(stateOpt.isPresent());
        ProcessStateView state = stateOpt.get();
        assertEquals("test_proc", state.processId());
        assertEquals("task-a", state.currentActivityId());
        assertEquals("active", state.lifecycleState());
        assertTrue(state.activityDurationsMs().containsKey("task-a"));
    }

    @Test
    public void shouldProduceStateCounts() {
        System.out.println("TC: produces state counts and verifies count store");

        String processId = "count_proc";
        produceEvent("pi-ct-1", processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:00:00Z");
        produceEvent("pi-ct-2", processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:01:00Z");
        produceEvent("pi-ct-1", processId, "validate", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:02:00Z");

        waitForCondition(() -> !queryService.countsForProcess(processId).isEmpty(), DEFAULT_TIMEOUT);

        List<ProcessStateCount> counts = queryService.countsForProcess(processId);
        assertTrue(counts.size() > 0);
        long total = counts.stream().mapToLong(ProcessStateCount::count).sum();
        assertTrue("expected at least one instance in count store", total >= 1);
    }

    @Test
    public void shouldProduceLatencySummaries() {
        System.out.println("TC: produces latency summaries from ACTIVITY_ENTERED + ACTIVITY_COMPLETED pairs");

        String processId = "lat_proc";
        produceEvent("pi-lt-1", processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:00:00Z");
        produceEvent("pi-lt-1", processId, "latency-task", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:01:00Z");
        produceEvent("pi-lt-1", processId, "latency-task", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-04-03T08:05:00Z");

        waitForCondition(() -> !queryService.latencyForProcess(processId).isEmpty(), DEFAULT_TIMEOUT);

        List<ActivityLatencySummary> latency = queryService.latencyForProcess(processId);
        assertTrue(latency.size() > 0);
        ActivityLatencySummary summary = latency.get(0);
        assertEquals("latency-task", summary.activityId());
        assertTrue(summary.sampleCount() >= 1);
        assertTrue(summary.maxDurationMs() >= 240000L);
    }

    @Test
    public void shouldDetectStuckInstances() {
        System.out.println("TC: detects stuck instances that have been active beyond a threshold");

        String instanceId = "pi-stuck-1";
        produceEvent(instanceId, "stuck_proc", "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:00:00Z");
        produceEvent(instanceId, "stuck_proc", "long-task", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:01:00Z");

        waitForCondition(() -> queryService.findInstance(instanceId)
                .map(s -> "active".equals(s.lifecycleState())).orElse(false), DEFAULT_TIMEOUT);

        List<StuckProcessInstance> stuck = queryService.stuckInstances("stuck_proc", 1);
        Optional<StuckProcessInstance> found = stuck.stream()
                .filter(s -> instanceId.equals(s.processInstanceId()))
                .findFirst();
        assertTrue(found.isPresent());
        assertEquals("active", found.get().lifecycleState());
    }

    @Test
    public void shouldMaterializeProcessStateFromUnkeyedLifecycleEvents() {
        System.out.println("TC: materializes process state from unkeyed lifecycle event values");

        String instanceId = "pi-unkeyed-1";
        ProcessEvent event = new ProcessEvent(
                instanceId, "unkeyed_proc", "start", "token", "corr",
                Map.of(), ProcessEvent.Status.STARTED, null,
                ProcessEvent.EventType.PROCESS_STARTED, "v1", "BK", "2026-04-03T08:00:00Z"
        );
        producer.send(new ProducerRecord<>(TOPICS.eventsTopic(), null, event.toJson()));
        producer.flush();

        waitForCondition(() -> queryService.findInstance(instanceId).isPresent(), DEFAULT_TIMEOUT);

        ProcessStateView state = queryService.findInstance(instanceId).orElseThrow();
        assertEquals("unkeyed_proc", state.processId());
        assertEquals(instanceId, state.processInstanceId());
    }

    private void produceEvent(String instanceId, String processId, String activityId,
                               ProcessEvent.EventType eventType, String timestamp) {
        ProcessEvent event = new ProcessEvent(
                instanceId, processId, activityId, "token", "corr",
                Map.of(), ProcessEvent.Status.STARTED, null,
                eventType, "v1", "BK", timestamp
        );
        producer.send(new ProducerRecord<>(TOPICS.eventsTopic(), instanceId, event.toJson()));
        producer.flush();
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
                    try { deleteRecursive(p); } catch (IOException ignored) {}
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
