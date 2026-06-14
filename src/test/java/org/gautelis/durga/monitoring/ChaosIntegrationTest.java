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

public class ChaosIntegrationTest extends KafkaIntegrationTestBase {
    private static final String SUFFIX = "-chaos-" + UUID.randomUUID().toString().substring(0, 8);
    private static final ProcessMonitoringTopology.MonitoringTopics TOPICS = new ProcessMonitoringTopology.MonitoringTopics(
            "process-events" + SUFFIX,
            "process-state" + SUFFIX,
            "process-state-counts" + SUFFIX,
            "process-active-state" + SUFFIX,
            "process-latency" + SUFFIX,
            "process-trends" + SUFFIX,
            "process-state-global-store" + SUFFIX,
            "process-state-counts-global-store" + SUFFIX,
            "process-active-state-global-store" + SUFFIX,
            "process-latency-global-store" + SUFFIX,
            "process-trends-global-store" + SUFFIX
    );
    private static String APP_ID;

    private KafkaProducer<String, String> producer;
    private KafkaStreams streams;
    private Path stateDir;

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
                    new NewTopic(TOPICS.trendsTopic(), 1, (short) 1)
            );
            admin.createTopics(topics).all().get();
        }
    }

    @Before
    public void setUp() throws IOException {
        stateDir = Files.createTempDirectory("kafka-streams-chaos-");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
        APP_ID = "chaos-it-app" + UUID.randomUUID().toString().substring(0, 8);
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
    public void shouldRecoverStateAfterTopologyRestart() {
        System.out.println("TC: recovers materialized process state after topology crash and restart");

        String instanceId = "pi-chaos-1";
        String processId = "chaos_proc";

        startTopology();
        produceCompleteFlow(instanceId, processId);
        awaitRunning();

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId)
                    .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
        }, Duration.ofSeconds(30));

        ProcessStateView before = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull(before);
        assertEquals("completed", before.lifecycleState());

        killTopology();

        startTopology();
        awaitRunning();

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId).isPresent();
        }, Duration.ofSeconds(30));

        ProcessStateView after = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull(after);
        assertEquals(before.lifecycleState(), after.lifecycleState());
        assertEquals(before.processInstanceId(), after.processInstanceId());
        assertEquals(before.processId(), after.processId());
    }

    @Test
    public void shouldNotDuplicateStateEntriesAfterRestart() {
        System.out.println("TC: verifies no duplicate state entries after topology restart");

        String processId = "dedup_proc";

        startTopology();
        produceCompleteFlow("pi-dedup-1", processId);
        awaitRunning();

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return !qs.countsForProcess(processId).isEmpty();
        }, Duration.ofSeconds(30));

        List<ProcessStateCount> beforeCounts = new ProcessMonitoringQueryService(streams, TOPICS)
                .countsForProcess(processId);
        long beforeTotal = beforeCounts.stream().mapToLong(ProcessStateCount::count).sum();
        assertTrue(beforeTotal > 0);

        killTopology();

        startTopology();
        awaitRunning();

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return !qs.countsForProcess(processId).isEmpty();
        }, Duration.ofSeconds(30));

        List<ProcessStateCount> afterCounts = new ProcessMonitoringQueryService(streams, TOPICS)
                .countsForProcess(processId);
        long afterTotal = afterCounts.stream().mapToLong(ProcessStateCount::count).sum();

        assertEquals("counts should not duplicate after restart", beforeTotal, afterTotal);
    }

    @Test
    public void shouldHandleMultipleRestarts() {
        System.out.println("TC: handles multiple topology restarts without corrupting state");

        String instanceId = "pi-mult-1";
        String processId = "mult_proc";

        for (int restart = 0; restart < 3; restart++) {
            startTopology();
            if (restart == 0) {
                produceCompleteFlow(instanceId, processId);
            }
            awaitRunning();

            int r = restart;
            waitForCondition(() -> {
                ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
                return qs.findInstance(instanceId)
                        .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
            }, Duration.ofSeconds(30));

            ProcessStateView state = new ProcessMonitoringQueryService(streams, TOPICS)
                    .findInstance(instanceId).orElse(null);
            assertNotNull("state should be present after restart #" + r, state);
            assertEquals("completed", state.lifecycleState());
            assertEquals(instanceId, state.processInstanceId());

            if (restart < 2) {
                killTopology();
            }
        }
    }

    private void startTopology() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = ProcessMonitoringTopology.buildTopology(TOPICS);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();
    }

    private void killTopology() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
            streams = null;
        }
    }

    private void awaitRunning() {
        waitForCondition(() -> streams != null && streams.state() == KafkaStreams.State.RUNNING, Duration.ofSeconds(30));
    }

    private void produceCompleteFlow(String instanceId, String processId) {
        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-04-03T08:00:00Z");
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-04-03T08:01:00Z");
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-04-03T08:05:00Z");
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.PROCESS_COMPLETED, "2026-04-03T08:06:00Z");
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
}
