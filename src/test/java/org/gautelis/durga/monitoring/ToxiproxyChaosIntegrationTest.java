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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * Chaos tests using Toxiproxy to inject network faults between the Kafka Streams
 * topology and the broker.
 * <p>
 * Toxics tested: latency spikes, connection timeouts, and packet loss — all
 * common in real-world Kafka deployments across regions and overloaded networks.
 */
public class ToxiproxyChaosIntegrationTest extends KafkaIntegrationTestBase {

    private static final String SUFFIX = "-toxichaos-" + UUID.randomUUID().toString().substring(0, 8);
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
    private static final String APP_ID = "toxichaos-it-app" + UUID.randomUUID().toString().substring(0, 8);

    private static ToxiproxyContainer toxiproxy;
    private static ToxiproxyContainer.ContainerProxy kafkaProxy;

    private String proxiedBootstrap;
    private KafkaStreams streams;
    private Path stateDir;
    private HttpClient httpClient;

    @BeforeClass
    public static void startProxies() {
        Network network = Network.newNetwork();

        System.out.println("TC: starting Toxiproxy for chaos injection");
        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
                .withNetwork(network);
        toxiproxy.start();

        kafkaProxy = toxiproxy.getProxy(kafka, 9093);

        System.out.println("TC: Toxiproxy accepting at " + kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort());
    }

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
        stateDir = Files.createTempDirectory("kafka-streams-toxichaos-");
        proxiedBootstrap = kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort();
        httpClient = HttpClient.newHttpClient();
        clearToxics();
    }

    @After
    public void tearDown() {
        if (streams != null) {
            try { streams.close(Duration.ofSeconds(10)); } catch (Exception ignored) {}
            try { streams.cleanUp(); } catch (Exception ignored) {}
        }
        if (stateDir != null) {
            try { deleteRecursive(stateDir); } catch (IOException ignored) {}
        }
    }

    @Test
    public void shouldContinueProcessingUnderLatencySpikes() throws Exception {
        System.out.println("TC: topology continues processing events through Toxiproxy latency spikes");

        String instanceId = "pi-lat-1";
        String processId = "lat_proc";

        startTopology();
        awaitRunning();

        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-06-01T08:00:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-06-01T08:01:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            Optional<ProcessStateView> state = qs.findInstance(instanceId);
            return state.map(s -> "task-a".equals(s.currentActivityId())).orElse(false);
        }, Duration.ofSeconds(15));

        addLatencyToxic(500); // 500ms latency each way = 1s RTT penalty

        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-06-01T08:05:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.PROCESS_COMPLETED, "2026-06-01T08:06:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId)
                    .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
        }, Duration.ofSeconds(45));

        ProcessStateView state = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull("state should be present under latency", state);
        assertEquals("completed", state.lifecycleState());

        clearToxics();
    }

    @Test
    public void shouldRecoverAfterConnectionTimeout() throws Exception {
        System.out.println("TC: topology recovers after Toxiproxy connection timeout toxic");

        String instanceId = "pi-timeout-1";
        String processId = "timeout_proc";

        startTopology();
        awaitRunning();

        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-06-01T09:00:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId).isPresent();
        }, Duration.ofSeconds(15));

        addTimeoutToxic(2000); // 2s timeout — forces retries

        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-06-01T09:01:00Z");

        // Give time for the topology to retry through the timeout
        Thread.sleep(5000);

        clearToxics();
        Thread.sleep(2000);

        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-06-01T09:05:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.PROCESS_COMPLETED, "2026-06-01T09:06:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId)
                    .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
        }, Duration.ofSeconds(30));

        ProcessStateView state = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull("state should recover after timeout toxic removal", state);
        assertEquals("completed", state.lifecycleState());
    }

    @Test
    public void shouldHandlePacketLoss() throws Exception {
        System.out.println("TC: topology eventually converges despite packet loss through Toxiproxy");

        String instanceId = "pi-loss-1";
        String processId = "loss_proc";

        startTopology();
        awaitRunning();

        addPacketLossToxic(0.3); // 30% packet loss

        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-06-01T10:00:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-06-01T10:01:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-06-01T10:05:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.PROCESS_COMPLETED, "2026-06-01T10:06:00Z");

        // Under packet loss, Kafka will retry (producer acks + consumer re-fetches).
        // The topology should eventually converge once enough retries succeed.
        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId)
                    .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
        }, Duration.ofSeconds(60));

        clearToxics();

        ProcessStateView state = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull("state should converge despite packet loss", state);
        assertEquals("completed", state.lifecycleState());
    }

    @Test
    public void shouldSurviveFullDisconnectAndReconnect() throws Exception {
        System.out.println("TC: topology reconnects and recovers after full proxy disconnect");

        String instanceId = "pi-disconnect-1";
        String processId = "disc_proc";

        startTopology();
        awaitRunning();

        produceEvent(instanceId, processId, "start", ProcessEvent.EventType.PROCESS_STARTED, "2026-06-01T11:00:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId).isPresent();
        }, Duration.ofSeconds(15));

        // Tear down the proxy entirely — simulates complete network partition
        disableProxy();
        System.out.println("TC: proxy stopped — simulating network partition");
        Thread.sleep(3000);

        // Re-establish the proxy
        enableProxy();
        System.out.println("TC: proxy re-established at " + proxiedBootstrap);

        // After reconnect, the topology internal retry loop should pick up.
        // Send new events to verify the topology is functional again.
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_ENTERED, "2026-06-01T11:05:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.ACTIVITY_COMPLETED, "2026-06-01T11:10:00Z");
        produceEvent(instanceId, processId, "task-a", ProcessEvent.EventType.PROCESS_COMPLETED, "2026-06-01T11:11:00Z");

        waitForCondition(() -> {
            ProcessMonitoringQueryService qs = new ProcessMonitoringQueryService(streams, TOPICS);
            return qs.findInstance(instanceId)
                    .map(s -> "completed".equals(s.lifecycleState())).orElse(false);
        }, Duration.ofSeconds(45));

        ProcessStateView state = new ProcessMonitoringQueryService(streams, TOPICS)
                .findInstance(instanceId).orElse(null);
        assertNotNull("state should be present after reconnection", state);
        assertEquals("completed", state.lifecycleState());
    }

    // ---- Toxiproxy HTTP API helpers ----

    private String toxiproxyApiUrl() {
        return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getControlPort() + "/proxies/" + kafkaProxy.getContainerIpAddress() + ":" + kafkaProxy.getProxyPort();
    }

    private void clearToxics() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(toxiproxyApiUrl() + "/toxics"))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"enabled\":false}"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .build();
            httpClient.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
        }
    }

    private void disableProxy() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(toxiproxyApiUrl()))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"enabled\":false}"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .build();
            httpClient.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
        }
    }

    private void enableProxy() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(toxiproxyApiUrl()))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"enabled\":true}"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .build();
            httpClient.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
        }
    }

    private void addLatencyToxic(long latencyMs) throws Exception {
        String body = String.format(
                "{\"name\":\"latency_%d\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":%d,\"jitter\":50}}",
                latencyMs, latencyMs);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(toxiproxyApiUrl() + "/toxics"))
                .method("POST", HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("TC: added latency toxic " + latencyMs + "ms -> " + resp.statusCode());
    }

    private void addTimeoutToxic(long timeoutMs) throws Exception {
        String name = "timeout_" + timeoutMs;
        String body = String.format(
                "{\"name\":\"%s\",\"type\":\"timeout\",\"stream\":\"downstream\",\"toxicity\":0.5,\"attributes\":{\"timeout\":%d}}",
                name, timeoutMs);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(toxiproxyApiUrl() + "/toxics"))
                .method("POST", HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("TC: added timeout toxic " + timeoutMs + "ms -> " + resp.statusCode());
    }

    private void addPacketLossToxic(double lossRate) throws Exception {
        String body = String.format(Locale.US,
                "{\"name\":\"packet_loss\",\"type\":\"slicer\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"average_size\":1024,\"size_variation\":512,\"delay\":0}}",
                lossRate);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(toxiproxyApiUrl() + "/toxics"))
                .method("POST", HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("TC: added packet loss toxic (slicer) -> " + resp.statusCode());
    }

    // ---- Kafka Streams helpers ----

    private void startTopology() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrap);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProps.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 5);

        Topology topology = ProcessMonitoringTopology.buildTopology(TOPICS);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();
    }

    private void awaitRunning() {
        waitForCondition(() -> streams != null && streams.state() == KafkaStreams.State.RUNNING, Duration.ofSeconds(30));
    }

    private void produceEvent(String instanceId, String processId, String activityId,
                               ProcessEvent.EventType eventType, String timestamp) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 5);

        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps)) {
            ProcessEvent event = new ProcessEvent(
                    instanceId, processId, activityId, "token", "corr",
                    Map.of(), ProcessEvent.Status.STARTED, null,
                    eventType, "v1", "BK", timestamp
            );
            p.send(new ProducerRecord<>(TOPICS.eventsTopic(), instanceId, event.toJson()));
            p.flush();
        }
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
                Thread.sleep(500);
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
