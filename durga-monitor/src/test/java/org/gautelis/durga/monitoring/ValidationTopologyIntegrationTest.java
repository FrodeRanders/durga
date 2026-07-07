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
import org.gautelis.durga.validation.ValidationResult;
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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ValidationTopologyIntegrationTest extends KafkaIntegrationTestBase {
    private static final String SUFFIX = "-val-" + UUID.randomUUID().toString().substring(0, 8);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    private static final String EVENTS_TOPIC = "process-events-valproc" + SUFFIX;
    private static final String VALIDATION_EVENTS_TOPIC = EVENTS_TOPIC + "-validation";
    private static final String RESULTS_TOPIC = "validation-results" + SUFFIX;
    private static final String RESULTS_STORE = "validation-results-store" + SUFFIX;
    private static final ValidationTopology.ValidationTopics TOPICS =
            new ValidationTopology.ValidationTopics(
                    null, Pattern.compile(Pattern.quote(VALIDATION_EVENTS_TOPIC)), EVENTS_TOPIC,
                    Pattern.compile(Pattern.quote(EVENTS_TOPIC)),
                    RESULTS_TOPIC, RESULTS_STORE, List.of("ts"));

    private KafkaProducer<String, String> producer;
    private KafkaStreams streams;
    private Path stateDir;
    private ValidationQueryService queryService;

    @BeforeClass
    public static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(
                    new NewTopic(VALIDATION_EVENTS_TOPIC, 1, (short) 1),
                    new NewTopic(EVENTS_TOPIC, 1, (short) 1),
                    new NewTopic(RESULTS_TOPIC, 1, (short) 1)
            )).all().get();
        }
    }

    @Before
    public void setUp() throws IOException {
        stateDir = Files.createTempDirectory("kafka-streams-val-");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "validation-it-" + UUID.randomUUID().toString().substring(0, 8));
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = ValidationTopology.buildTopology(TOPICS);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();
        waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, DEFAULT_TIMEOUT);

        queryService = new ValidationQueryService(streams, RESULTS_STORE);
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
    public void shouldMarkEqualWhenCandidateMatchesPriorRegardlessOfArrivalOrder() {
        System.out.println("TC: candidate arriving before prior still resolves to EQUAL once prior output arrives");
        String instance = "pi-eq";
        produceCandidateSuccess(instance, Map.of("amount", 100));
        producePriorCompleted(instance, Map.of("amount", 100));

        waitForStatus(instance, ValidationResult.MatchStatus.EQUAL);
        ValidationResult result = result(instance).orElseThrow();
        assertTrue(result.diffs().isEmpty());
        assertEquals("candidate", result.candidateVersion());
    }

    @Test
    public void shouldMarkDiffWithLocatedDifference() {
        System.out.println("TC: differing candidate output is DIFF with the located difference");
        String instance = "pi-diff";
        producePriorCompleted(instance, Map.of("amount", 100));
        produceCandidateSuccess(instance, Map.of("amount", 200));

        waitForStatus(instance, ValidationResult.MatchStatus.DIFF);
        ValidationResult result = result(instance).orElseThrow();
        assertEquals(1, result.diffs().size());
        assertEquals("amount", result.diffs().get(0).path());
    }

    @Test
    public void shouldMarkPriorMissingWhenNoProductionOutput() {
        System.out.println("TC: candidate with no prior output is PRIOR_MISSING");
        String instance = "pi-missing";
        produceCandidateSuccess(instance, Map.of("amount", 5));

        waitForStatus(instance, ValidationResult.MatchStatus.PRIOR_MISSING);
    }

    @Test
    public void shouldMarkCandidateError() {
        System.out.println("TC: candidate error is CANDIDATE_ERROR even when prior output exists");
        String instance = "pi-err";
        producePriorCompleted(instance, Map.of("amount", 100));
        produceCandidateError(instance);

        waitForStatus(instance, ValidationResult.MatchStatus.CANDIDATE_ERROR);
        ValidationResult result = result(instance).orElseThrow();
        assertEquals("VALIDATION_CANDIDATE_FAILED", result.candidateErrorCode());
    }

    @Test
    public void shouldIgnoreConfiguredPaths() {
        System.out.println("TC: differences under an ignore path do not count as a divergence");
        String instance = "pi-ign";
        producePriorCompleted(instance, Map.of("amount", 100, "ts", "earlier"));
        produceCandidateSuccess(instance, Map.of("amount", 100, "ts", "later"));

        waitForStatus(instance, ValidationResult.MatchStatus.EQUAL);
    }

    @Test
    public void shouldSummarizePerTask() {
        System.out.println("TC: per-task summary counts equal and diff outcomes");
        producePriorCompleted("pi-s1", Map.of("amount", 1));
        produceCandidateSuccess("pi-s1", Map.of("amount", 1));
        producePriorCompleted("pi-s2", Map.of("amount", 1));
        produceCandidateSuccess("pi-s2", Map.of("amount", 2));

        waitForStatus("pi-s1", ValidationResult.MatchStatus.EQUAL);
        waitForStatus("pi-s2", ValidationResult.MatchStatus.DIFF);

        waitForCondition(() -> {
            List<ValidationSummary> summaries = queryService.summaryForProcess("valproc");
            return summaries.stream().anyMatch(s -> "transform".equals(s.taskId())
                    && s.equal() >= 1 && s.diff() >= 1);
        }, DEFAULT_TIMEOUT);
    }

    private void producePriorCompleted(String instance, Map<String, Object> payload) {
        ProcessEvent event = new ProcessEvent(
                instance, "valproc", "transform", "tok", "corr",
                payload, ProcessEvent.Status.COMPLETED, null,
                ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", "BK", "2026-07-01T10:00:00Z");
        producer.send(new ProducerRecord<>(EVENTS_TOPIC, instance, event.toJson()));
        producer.flush();
    }

    private void produceCandidateSuccess(String instance, Map<String, Object> outputPayload) {
        ProcessEvent event = new ProcessEvent(
                instance, "valproc", "transform", "tok", "corr",
                outputPayload, ProcessEvent.Status.COMPLETED, null,
                ProcessEvent.EventType.ACTIVITY_COMPLETED, "candidate", "BK", "2026-07-01T10:00:01Z");
        producer.send(new ProducerRecord<>(VALIDATION_EVENTS_TOPIC, instance, event.toJson()));
        producer.flush();
    }

    private void produceCandidateError(String instance) {
        ProcessEvent event = new ProcessEvent(
                instance, "valproc", "transform", "tok", "corr",
                null, ProcessEvent.Status.FAILED,
                new ProcessEvent.ErrorInfo("boom", "VALIDATION_CANDIDATE_FAILED"),
                ProcessEvent.EventType.PROCESS_FAILED, "candidate", "BK", "2026-07-01T10:00:01Z");
        producer.send(new ProducerRecord<>(VALIDATION_EVENTS_TOPIC, instance, event.toJson()));
        producer.flush();
    }

    private Optional<ValidationResult> result(String instance) {
        return queryService.findResult("valproc", "transform", instance);
    }

    private void waitForStatus(String instance, ValidationResult.MatchStatus expected) {
        waitForCondition(() -> result(instance)
                .map(r -> r.matchStatus() == expected)
                .orElse(false), DEFAULT_TIMEOUT);
        assertEquals(expected, result(instance).orElseThrow().matchStatus());
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
