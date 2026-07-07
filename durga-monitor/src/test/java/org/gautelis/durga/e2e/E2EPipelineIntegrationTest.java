package org.gautelis.durga.e2e;

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
import org.gautelis.durga.monitoring.*;
import org.gautelis.durga.tools.BpmnScaffolder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * End-to-end integration test verifying:
 * <p>
 * 1. The scaffolder generates correct category-specific lifecycle events
 *    (GATEWAY_TAKEN for route, ACTIVITY_ESCALATED for validate,
 *     PROCESS_COMPLETED for aggregate absorbing an instance into an open window).
 * 2. The monitoring topology correctly materializes state from the new
 *    event types.
 * 3. Vannak data-individual metadata events carry plugin operation metadata.
 */
public class E2EPipelineIntegrationTest extends KafkaIntegrationTestBase {
    private static final String SUFFIX = "-e2e-" + UUID.randomUUID().toString().substring(0, 8);
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

    private static final String VANNAK_TOPIC = "vannak-metadata-events" + SUFFIX;

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
                    new NewTopic(TOPICS.modelsTopic(), 1, (short) 1),
                    new NewTopic(VANNAK_TOPIC, 1, (short) 1)
            );
            admin.createTopics(topics).all().get();
        }
    }

    @Before
    public void setUp() throws IOException {
        stateDir = Files.createTempDirectory("kafka-streams-e2e-");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "monitoring-e2e-" + UUID.randomUUID().toString().substring(0, 8));
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = ProcessMonitoringTopology.buildTopology(TOPICS);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();
        waitForKafkaStreams(() -> streams.state() == KafkaStreams.State.RUNNING, DEFAULT_TIMEOUT);

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

    // ---------- scaffolder verification ----------

    @Test
    public void shouldGeneratePluginExecutorsWithCategorySpecificEvents() throws Exception {
        System.out.println("TC: e2e pipeline generates executors with GATEWAY_TAKEN, ACTIVITY_ESCALATED, and PROCESS_COMPLETED");

        Path outputDir = Files.createTempDirectory("durga-e2e-");

        BpmnScaffolder.main(new String[]{
                fixturePath("e2e_pipeline.bpmn").toString(),
                "--out", outputDir.toString(),
                "--package", "org.example.e2e",
                "--process-id", "e2e_pipeline"
        });

        Path pkg = outputDir.resolve("src/main/java/org/example/e2e");

        // route plugin → GATEWAY_TAKEN
        String routeContent = Files.readString(pkg.resolve("RouteDecisionPluginExecutor.java"));
        assertTrue("route executor missing GATEWAY_TAKEN", routeContent.contains("ProcessEvent.EventType.GATEWAY_TAKEN"));
        assertTrue("route executor missing Status.COMPLETED", routeContent.contains("ProcessEvent.Status.COMPLETED"));

        // validate plugin → ACTIVITY_ESCALATED
        String validateContent = Files.readString(pkg.resolve("ValidateHighValuePluginExecutor.java"));
        assertTrue("validate executor missing ACTIVITY_ESCALATED", validateContent.contains("ProcessEvent.EventType.ACTIVITY_ESCALATED"));
        assertTrue("validate executor missing Status.ESCALATED", validateContent.contains("ProcessEvent.Status.ESCALATED"));
        assertTrue("validate executor missing VALIDATION_FAILED", validateContent.contains("VALIDATION_FAILED"));

        // aggregate plugin → an instance absorbed into an open window is terminal (PROCESS_COMPLETED)
        String aggregateContent = Files.readString(pkg.resolve("AggregateHighValuePluginExecutor.java"));
        assertTrue("aggregate executor missing PROCESS_COMPLETED", aggregateContent.contains("ProcessEvent.EventType.PROCESS_COMPLETED"));
        assertTrue("aggregate executor missing Status.COMPLETED", aggregateContent.contains("ProcessEvent.Status.COMPLETED"));
    }

    // ---------- monitoring topology verification ----------

    @Test
    public void shouldHandleGatewayTakenEvents() {
        System.out.println("TC: monitoring topology correctly tracks GATEWAY_TAKEN lifecycle events");

        String instanceId = "pi-e2e-gateway-1";
        String processId = "e2e_gateway_test";
        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-07-01T10:00:00Z");
        produceEvent(instanceId, processId, "route_decision", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:01:00Z");
        produceEvent(instanceId, processId, "route_decision", ProcessEvent.EventType.GATEWAY_TAKEN, "2026-07-01T10:01:01Z");

        waitForKafkaStreams(() -> queryService.findInstance(instanceId).isPresent(), DEFAULT_TIMEOUT);

        ProcessStateView state = queryService.findInstance(instanceId).orElseThrow();
        assertEquals("e2e_gateway_test", state.processId());
        assertEquals("active", state.lifecycleState());
        assertTrue("gateway activity duration is recorded", state.activityDurationsMs().containsKey("route_decision"));
    }

    @Test
    public void shouldHandleActivityEscalatedEvents() {
        System.out.println("TC: monitoring topology correctly tracks ACTIVITY_ESCALATED lifecycle events");

        String instanceId = "pi-e2e-escalated-1";
        String processId = "e2e_escalated_test";
        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-07-01T10:00:00Z");
        produceEvent(instanceId, processId, "validate_high_value", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:02:00Z");
        produceEvent(instanceId, processId, "validate_high_value", ProcessEvent.EventType.ACTIVITY_ESCALATED, "2026-07-01T10:02:05Z");

        waitForKafkaStreams(() -> queryService.findInstance(instanceId).isPresent(), DEFAULT_TIMEOUT);

        ProcessStateView state = queryService.findInstance(instanceId).orElseThrow();
        assertEquals("e2e_escalated_test", state.processId());
        assertEquals("active", state.lifecycleState());
        assertTrue("escalated activity duration is recorded", state.activityDurationsMs().containsKey("validate_high_value"));
    }

    @Test
    public void shouldHandleActivityEnteredThenCompletedEvents() {
        System.out.println("TC: monitoring topology tracks ACTIVITY_ENTERED latency on ACTIVITY_COMPLETED");

        String instanceId = "pi-e2e-entered-1";
        String processId = "e2e_entered_test";
        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-07-01T10:00:00Z");
        produceEvent(instanceId, processId, "aggregate_high_value", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:03:00Z");
        produceEvent(instanceId, processId, "aggregate_high_value", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-07-01T10:03:30Z");

        waitForKafkaStreams(() -> !queryService.latencyForProcess(processId).isEmpty(), DEFAULT_TIMEOUT);

        List<ActivityLatencySummary> latency = queryService.latencyForProcess(processId);
        assertTrue("latency summary produced", latency.size() > 0);
        ActivityLatencySummary summary = latency.get(0);
        assertEquals("aggregate_high_value", summary.activityId());
        assertTrue("at least one sample recorded", summary.sampleCount() >= 1);
        assertTrue("latency recorded", summary.maxDurationMs() >= 30000L);
    }

    @Test
    public void shouldHandleMultipleEventTypesInLifecycle() {
        System.out.println("TC: monitoring topology tracks a full lifecycle with mixed event types");

        String instanceId = "pi-e2e-lifecycle-1";
        String processId = "e2e_lifecycle_test";

        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-07-01T10:00:00Z");

        produceEvent(instanceId, processId, "transform_order", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:01:00Z");
        produceEvent(instanceId, processId, "transform_order", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-07-01T10:01:30Z");

        produceEvent(instanceId, processId, "coerce_types", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:01:31Z");
        produceEvent(instanceId, processId, "coerce_types", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-07-01T10:01:45Z");

        produceEvent(instanceId, processId, "route_decision", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:01:46Z");
        produceEvent(instanceId, processId, "route_decision", ProcessEvent.EventType.GATEWAY_TAKEN, "2026-07-01T10:01:47Z");

        produceEvent(instanceId, processId, "enrich_high_value", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:01:48Z");
        produceEvent(instanceId, processId, "enrich_high_value", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-07-01T10:02:00Z");

        produceEvent(instanceId, processId, "validate_high_value", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-07-01T10:02:01Z");
        produceEvent(instanceId, processId, "validate_high_value", ProcessEvent.EventType.ACTIVITY_ESCALATED, "2026-07-01T10:02:02Z");

        waitForKafkaStreams(() -> queryService.findInstance(instanceId).isPresent(), DEFAULT_TIMEOUT);

        ProcessStateView state = queryService.findInstance(instanceId).orElseThrow();
        assertEquals("e2e_lifecycle_test", state.processId());
        assertEquals("active", state.lifecycleState());
        assertTrue("transform duration recorded", state.activityDurationsMs().containsKey("transform_order"));
        assertTrue("coerce duration recorded", state.activityDurationsMs().containsKey("coerce_types"));
        assertTrue("route duration recorded", state.activityDurationsMs().containsKey("route_decision"));
        assertTrue("enrich duration recorded", state.activityDurationsMs().containsKey("enrich_high_value"));
        assertTrue("validate duration recorded", state.activityDurationsMs().containsKey("validate_high_value"));
    }

    @Test
    public void shouldParseAlarmConfigsFromBpmnModel() throws Exception {
        System.out.println("TC: BPMN model contains alarm configs at process, inherited, and activity levels");

        String bpmnXml = Files.readString(fixturePath("e2e_pipeline.bpmn"));
        List<org.gautelis.durga.monitoring.AlarmConfig> configs =
                org.gautelis.durga.monitoring.BpmnAlarmConfigParser.parse(bpmnXml);

        assertFalse("expected alarm configs from BPMN", configs.isEmpty());

        // activity-level: validate-escalation on validate_high_value
        assertTrue("missing activity-level HARD_ERROR alarm",
                configs.stream().anyMatch(c ->
                        "e2e_pipeline:validate-escalation".equals(c.id())
                        && "validate_high_value".equals(c.activityId())
                        && c.syndrome() == org.gautelis.durga.monitoring.AlarmSyndrome.HARD_ERROR));

        // process-level inherited: *default (COUNTED, threshold=5) for every activity
        assertTrue("missing inherited COUNTED alarm",
                configs.stream().anyMatch(c ->
                        c.id().endsWith(":default")
                        && c.syndrome() == org.gautelis.durga.monitoring.AlarmSyndrome.COUNTED
                        && c.threshold() == 5));

        // process-level aggregate: $burst (SLIDING_WINDOW, threshold=3, 60s window)
        assertTrue("missing aggregate SLIDING_WINDOW alarm",
                configs.stream().anyMatch(c ->
                        c.id().endsWith(":burst")
                        && c.activityId() == null
                        && c.syndrome() == org.gautelis.durga.monitoring.AlarmSyndrome.SLIDING_WINDOW));
    }

    // ---------- helpers ----------

    private void produceEvent(String instanceId, String processId, String activityId,
                               ProcessEvent.EventType eventType, String timestamp) {
        ProcessEvent.Status status = inferStatus(eventType);
        Map<String, Object> payload = eventType == ProcessEvent.EventType.ACTIVITY_ESCALATED
                ? Map.of("_payloadRedacted", true)
                : Map.of("order_id", 123, "amount", 750.0, "customer_email", "alice@example.com", "status", "pending");
        ProcessEvent event = new ProcessEvent(
                instanceId, processId, activityId, "token-" + UUID.randomUUID().toString().substring(0, 6),
                "corr-e2e", payload, status, null,
                eventType, "v1", "bk-e2e", timestamp
        );
        producer.send(new ProducerRecord<>(TOPICS.eventsTopic(), instanceId, event.toJson()));
        producer.flush();
    }

    private static ProcessEvent.Status inferStatus(ProcessEvent.EventType eventType) {
        return switch (eventType) {
            case PROCESS_STARTED, ACTIVITY_ENTERED -> ProcessEvent.Status.STARTED;
            case ACTIVITY_COMPLETED, GATEWAY_TAKEN -> ProcessEvent.Status.COMPLETED;
            case ACTIVITY_ESCALATED -> ProcessEvent.Status.ESCALATED;
            case PROCESS_COMPLETED -> ProcessEvent.Status.COMPLETED;
            case ACTIVITY_CANCELLED -> ProcessEvent.Status.CANCELLED;
            case PROCESS_FAILED -> ProcessEvent.Status.FAILED;
        };
    }

    static void waitForKafkaStreams(Callable<Boolean> condition, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.call()) return;
            } catch (Exception ignored) {}
            try { Thread.sleep(200); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        fail("Condition not met within timeout " + timeout);
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
        try { return Duration.ofSeconds(Long.parseLong(val)); } catch (NumberFormatException e) { return Duration.ofSeconds(60); }
    }

    /**
     * Resolves this module's {@code e2e_pipeline.bpmn} test fixture.
     * <p>
     * Note: this fixture intentionally diverges from the runnable demo at
     * {@code durga-tools/src/test/resources/bpmn/e2e_pipeline.bpmn}. It keeps the inline
     * {@code route_decision} (field-router) task on purpose, to verify that the scaffolder
     * emits route-category lifecycle events ({@code GATEWAY_TAKEN}). The tools copy removed
     * that task because a routing plugin's key output is not a payload; the framework now
     * guards against payload loss regardless (see {@code PluginResult.OutputDisposition}).
     */
    private static Path fixturePath(String fileName) {
        Path modulePath = Path.of("src/test/resources", fileName);
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        Path repoRootPath = Path.of("durga-monitor/src/test/resources", fileName);
        if (Files.exists(repoRootPath)) {
            return repoRootPath;
        }
        return modulePath;
    }
}
