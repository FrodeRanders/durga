package org.gautelis.durga;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionalKafkaIntegrationTest extends KafkaIntegrationTestBase {
    private static final String SUFFIX = "-tx-" + UUID.randomUUID().toString().substring(0, 8);

    @Test(timeout = 180_000)
    public void abortedTransactionsAreInvisibleToReadCommittedConsumers() {
        System.out.println("TC: aborted Kafka transactions are invisible to read_committed consumers");
        String outputTopic = "tx-output-abort" + SUFFIX;
        createTopic(outputTopic);

        KafkaProducer<String, String> producer = transactionalProducer("abort-visibility" + SUFFIX);
        try {
            producer.initTransactions();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(outputTopic, "k", "aborted"));
            producer.abortTransaction();

            try (KafkaConsumer<String, String> consumer = readCommittedConsumer("abort-visibility-reader" + SUFFIX)) {
                consumer.subscribe(List.of(outputTopic));
                assertFalse("aborted output must not be visible", pollForAny(consumer, Duration.ofSeconds(3)));
            }

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(outputTopic, "k", "committed"));
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(5));
        }

        try (KafkaConsumer<String, String> consumer = readCommittedConsumer("commit-visibility-reader" + SUFFIX)) {
            consumer.subscribe(List.of(outputTopic));
            ConsumerRecord<String, String> record = pollOne(consumer, Duration.ofSeconds(10));
            assertEquals("committed", record.value());
        }
    }

    @Test(timeout = 180_000)
    public void outputAndConsumedOffsetCommitAtomicallyInTransaction() {
        System.out.println("TC: Kafka transaction atomically commits output records and the consumed input offset");
        String inputTopic = "tx-input-offset" + SUFFIX;
        String outputTopic = "tx-output-offset" + SUFFIX;
        createTopic(inputTopic);
        createTopic(outputTopic);

        try (KafkaProducer<String, String> feeder = plainProducer()) {
            feeder.send(new ProducerRecord<>(inputTopic, "instance-1", "input-1"));
            feeder.flush();
        }

        String groupId = "tx-offset-group" + SUFFIX;
        KafkaProducer<String, String> workerProducer = transactionalProducer("offset-commit" + SUFFIX);
        try (KafkaConsumer<String, String> workerConsumer = readCommittedConsumer(groupId)) {
            workerConsumer.subscribe(List.of(inputTopic));
            ConsumerRecord<String, String> input = pollOne(workerConsumer, Duration.ofSeconds(10));

            workerProducer.initTransactions();
            workerProducer.beginTransaction();
            workerProducer.send(new ProducerRecord<>(outputTopic, input.key(), "output-1"));
            workerProducer.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(input.topic(), input.partition()),
                            new OffsetAndMetadata(input.offset() + 1)),
                    workerConsumer.groupMetadata()
            );
            workerProducer.commitTransaction();
        } finally {
            workerProducer.close(Duration.ofSeconds(5));
        }

        try (KafkaConsumer<String, String> outputReader = readCommittedConsumer("tx-output-reader" + SUFFIX)) {
            outputReader.subscribe(List.of(outputTopic));
            ConsumerRecord<String, String> output = pollOne(outputReader, Duration.ofSeconds(10));
            assertEquals("output-1", output.value());
        }

        try (KafkaConsumer<String, String> resumedConsumer = readCommittedConsumer(groupId)) {
            resumedConsumer.subscribe(List.of(inputTopic));
            assertFalse("input offset must be committed by the transaction",
                    pollForAny(resumedConsumer, Duration.ofSeconds(3)));
        }
    }

    @Test(timeout = 180_000)
    public void reusedTransactionIdFencesOlderProducer() {
        System.out.println("TC: Kafka fences an older transactional producer when the same transaction id is reused");
        String outputTopic = "tx-output-fence" + SUFFIX;
        createTopic(outputTopic);

        String transactionId = "fence" + SUFFIX;
        KafkaProducer<String, String> first = transactionalProducer(transactionId);
        KafkaProducer<String, String> second = transactionalProducer(transactionId);
        try {
            first.initTransactions();
            first.beginTransaction();
            first.send(new ProducerRecord<>(outputTopic, "k", "from-first"));

            second.initTransactions();
            second.beginTransaction();
            second.send(new ProducerRecord<>(outputTopic, "k", "from-second"));

            try {
                first.commitTransaction();
                fail("first producer should have been fenced");
            } catch (ProducerFencedException expected) {
                assertTrue(expected.getMessage() == null || !expected.getMessage().isBlank());
            } catch (KafkaException expected) {
                assertTrue(expected.getMessage() == null || expected.getMessage().contains("fenc"));
            }
            second.abortTransaction();
        } finally {
            first.close(Duration.ofSeconds(5));
            second.close(Duration.ofSeconds(5));
        }
    }

    private static void createTopic(String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new AssertionError("failed to create Kafka topic " + topic, e);
        }
    }

    private static KafkaProducer<String, String> plainProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<String, String> transactionalProducer(String transactionId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> readCommittedConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        return new KafkaConsumer<>(props);
    }

    private static ConsumerRecord<String, String> pollOne(
            KafkaConsumer<String, String> consumer,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        fail("expected one Kafka record");
        throw new AssertionError("unreachable");
    }

    private static boolean pollForAny(KafkaConsumer<String, String> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (!consumer.poll(Duration.ofMillis(250)).isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
