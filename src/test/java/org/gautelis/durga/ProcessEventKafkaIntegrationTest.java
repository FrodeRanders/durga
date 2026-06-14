package org.gautelis.durga;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.*;

public class ProcessEventKafkaIntegrationTest extends KafkaIntegrationTestBase {
    private static final String TOPIC = "process-event-it-" + UUID.randomUUID().toString().substring(0, 8);
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    @BeforeClass
    public static void createTopic() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Before
    public void setUp() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(TOPIC));
    }

    @After
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    public void shouldProduceAndConsumeSingleEvent() {
        System.out.println("TC: produces and consumes a single ProcessEvent over Kafka with JSON round-trip integrity");
        ProcessEvent event = new ProcessEvent(
                "pi-1", "invoice_receipt", "review_invoice", "token-1", "corr-1",
                Map.of("amount", 100), ProcessEvent.Status.COMPLETED,
                new ProcessEvent.ErrorInfo("err", "TASK_FAILED"),
                ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", "INV-1",
                "2026-04-03T08:03:00Z"
        );
        producer.send(new ProducerRecord<>(TOPIC, event.processInstanceId(), event.toJson()));
        producer.flush();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        String consumedJson = records.iterator().next().value();
        ProcessEvent parsed = ProcessEvent.fromJson(consumedJson);
        assertEquals("pi-1", parsed.processInstanceId());
        assertEquals("invoice_receipt", parsed.processId());
        assertEquals(ProcessEvent.EventType.ACTIVITY_COMPLETED, parsed.eventType());
        assertNotNull(parsed.error());
        assertEquals("TASK_FAILED", parsed.error().code());
    }

    @Test
    public void shouldProduceAndConsumeMultipleEvents() {
        System.out.println("TC: produces and consumes multiple ProcessEvents over Kafka preserving integrity");
        List<String> expectedIds = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String id = "pi-" + i;
            expectedIds.add(id);
            ProcessEvent event = new ProcessEvent(
                    id, "test_proc", "act-" + i, "token-" + i, "corr-" + i,
                    Map.of("seq", i), ProcessEvent.Status.COMPLETED,
                    null, ProcessEvent.EventType.ACTIVITY_COMPLETED,
                    "v1", "BK-" + i, "2026-04-03T08:0" + i + ":00Z"
            );
            producer.send(new ProducerRecord<>(TOPIC, id, event.toJson()));
        }
        producer.flush();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(5, records.count());

        Set<String> consumedIds = new HashSet<>();
        for (var record : records) {
            ProcessEvent parsed = ProcessEvent.fromJson(record.value());
            consumedIds.add(parsed.processInstanceId());
            assertNotNull(parsed.processId());
            assertNotNull(parsed.activityId());
            assertEquals(ProcessEvent.EventType.ACTIVITY_COMPLETED, parsed.eventType());
        }
        assertEquals(5, consumedIds.size());
    }

    @Test
    public void shouldRoundTripFromJsonToJson() {
        System.out.println("TC: verifies fromJson/toJson round-trip produces correct payloads");
        ProcessEvent original = new ProcessEvent(
                "pi-r", "roundtrip_proc", "task_r", "token-r", "corr-r",
                Map.of("key", "value"), ProcessEvent.Status.STARTED, null
        );

        producer.send(new ProducerRecord<>(TOPIC, original.processInstanceId(), original.toJson()));
        producer.flush();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count());

        ProcessEvent parsed = ProcessEvent.fromJson(records.iterator().next().value());
        String reSerialized = parsed.toJson();
        ProcessEvent doubleParsed = ProcessEvent.fromJson(reSerialized);

        assertEquals(original.processInstanceId(), doubleParsed.processInstanceId());
        assertEquals(original.eventType(), doubleParsed.eventType());
        assertEquals("value", doubleParsed.payload().get("key"));
    }
}
