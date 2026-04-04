package org.gautelis.durga;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Small Kafka-backed state helper used by generated coordination code.
 * <p>
 * The store writes complete {@link ProcessState} snapshots and reads them back by scanning the
 * compacted topic to the current end offset.
 */
public class ProcessStateStore implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    /**
     * Creates a state helper bound to the given bootstrap servers and topic.
     *
     * @param bootstrapServers Kafka bootstrap server list
     * @param topic compacted topic used for process state snapshots
     */
    public ProcessStateStore(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "process-state-reader");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    /**
     * Writes the latest snapshot for a process instance.
     *
     * @param state state snapshot to persist
     */
    public void save(ProcessState state) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, state.processInstanceId(), state.toJson());
        producer.send(record);
        producer.flush();
    }

    /**
     * Reads the latest visible snapshot for a process instance by scanning the topic up to the
     * current end offsets.
     *
     * @param processInstanceId process instance identifier
     * @param timeout maximum scan duration
     * @return latest visible snapshot, or {@code null} if no state was found
     */
    public ProcessState loadLatest(String processInstanceId, Duration timeout) {
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();

        if (partitions.isEmpty()) {
            return null;
        }

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        // This intentionally does a bounded full scan because the store is used as a simple
        // compacted-topic helper, not as a high-throughput query engine.
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        Map<TopicPartition, Long> lastOffsets = new HashMap<>();
        ProcessState latest = null;

        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (var record : records) {
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                lastOffsets.put(partition, record.offset());
                if (processInstanceId.equals(record.key())) {
                    latest = ProcessState.fromJson(record.value());
                }
            }

            boolean caughtUp = endOffsets.entrySet().stream().allMatch(entry -> {
                long last = lastOffsets.getOrDefault(entry.getKey(), -1L);
                return last >= entry.getValue() - 1;
            });

            // Stop once every assigned partition has been scanned up to its current end offset;
            // at that point "latest" really is the latest visible value for the key.
            if (caughtUp) {
                return latest;
            }
        }

        return latest;
    }

    /**
     * Closes the underlying producer and consumer.
     */
    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
