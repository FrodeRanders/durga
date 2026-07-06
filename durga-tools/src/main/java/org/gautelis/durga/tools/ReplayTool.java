package org.gautelis.durga.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Operator-facing tool for replaying process events from dead-letter queues,
 * specific instances, or topic offset ranges with idempotency and lineage tracking.
 *
 * <pre>
 * Commands:
 *   replay-dlq    &lt;dlq-topic>         Replay records from a dead-letter topic
 *   replay-offset &lt;topic> &lt;part> &lt;from> &lt;to>  Replay a range of offsets
 *   dry-run       &lt;dlq-topic/offset>  Preview which records would be replayed
 *   inspect-dlq   &lt;dlq-topic>         List DLQ records without replaying
 * </pre>
 */
public final class ReplayTool {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayTool.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ReplayTool() {
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];
        String bootstrap = resolveArg(args, "--bootstrap", "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");

        try {
            long timeoutSeconds = parseLongArg(resolveArg(args, "--timeout", null, "60"), "--timeout");
            switch (command) {
                case "replay-dlq" -> {
                    if (args.length < 2) {
                        System.err.println("Missing DLQ topic name");
                        System.exit(1);
                    }
                    String dlqTopic = args[1];
                    boolean dryRun = hasFlag(args, "--dry-run");
                    replayDlq(bootstrap, dlqTopic, timeoutSeconds, dryRun);
                }
                case "replay-offset" -> {
                    if (args.length < 5) {
                        System.err.println("Missing topic/partition/from/to arguments");
                        System.exit(1);
                    }
                    String topic = args[1];
                    int partition = (int) parseLongArg(args[2], "partition");
                    long fromOffset = parseLongArg(args[3], "from-offset");
                    long toOffset = parseLongArg(args[4], "to-offset");
                    boolean dryRun = hasFlag(args, "--dry-run");
                    replayOffset(bootstrap, topic, partition, fromOffset, toOffset, timeoutSeconds, dryRun);
                }
                case "inspect-dlq" -> {
                    if (args.length < 2) {
                        System.err.println("Missing DLQ topic name");
                        System.exit(1);
                    }
                    inspectDlq(bootstrap, args[1], timeoutSeconds);
                }
                case "dry-run" -> {
                    if (args.length < 2) {
                        System.err.println("Missing topic pattern");
                        System.exit(1);
                    }
                    dryRun(bootstrap, args[1], timeoutSeconds);
                }
                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            LOG.error("Replay failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void replayDlq(String bootstrap, String dlqTopic, long timeoutSeconds, boolean dryRun) {
        LOG.info("{} replay from DLQ topic '{}'", dryRun ? "Dry-run" : "Replaying", dlqTopic);
        ReplayPlan plan = new ReplayPlan("dlq:" + dlqTopic);
        Properties consumerProps = consumerProperties(bootstrap, "durga-replay-dlq-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = partitionsForTopic(consumer, dlqTopic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;

            while (hasRemaining(consumer, endOffsets) && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        ProcessEvent event = parseReplayEvent(record.value());
                        plan.add(new ReplayItem(event, dlqTopic, record.partition(), record.offset(),
                                record.key(), record.value()));
                    } catch (Exception e) {
                        LOG.warn("Skipping unparseable DLQ record at offset {}: {}", record.offset(), e.getMessage());
                    }
                }
            }
        }

        if (plan.isEmpty()) {
            LOG.info("No records found in DLQ topic '{}'", dlqTopic);
            return;
        }

        plan.print(System.out);
        if (dryRun) {
            LOG.info("Dry-run complete. {} records would be replayed. Use without --dry-run to execute.", plan.size());
            return;
        }

        executeReplay(bootstrap, dlqTopic, plan);
    }

    private static void replayOffset(String bootstrap, String topic, int partition,
                                      long fromOffset, long toOffset, long timeoutSeconds, boolean dryRun) {
        LOG.info("{} replay from {}:{}-{} ({})", dryRun ? "Dry-run" : "Replaying",
                topic, partition, fromOffset, toOffset);
        ReplayPlan plan = new ReplayPlan("offset:" + topic + "/" + partition + ":" + fromOffset + "-" + toOffset);
        Properties consumerProps = consumerProperties(bootstrap, "durga-replay-offset-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, fromOffset);

            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
            while (consumer.position(tp) < toOffset && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                boolean reachedEnd = false;
                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() >= toOffset) {
                        reachedEnd = true;
                        break;
                    }
                    try {
                        ProcessEvent event = ProcessEvent.fromJson(record.value());
                        plan.add(new ReplayItem(event, topic, partition, record.offset(),
                                record.key(), record.value()));
                    } catch (Exception e) {
                        LOG.warn("Skipping unparseable record at offset {}: {}", record.offset(), e.getMessage());
                    }
                }
                if (reachedEnd) {
                    break;
                }
            }
        }

        if (plan.isEmpty()) {
            LOG.info("No records found in offset range");
            return;
        }

        plan.print(System.out);
        if (dryRun) {
            LOG.info("Dry-run complete. {} records would be replayed. Use without --dry-run to execute.", plan.size());
            return;
        }

        executeReplay(bootstrap, topic, plan);
    }

    private static void inspectDlq(String bootstrap, String dlqTopic, long timeoutSeconds) {
        LOG.info("Inspecting DLQ topic '{}'", dlqTopic);
        Properties consumerProps = consumerProperties(bootstrap, "durga-inspect-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = partitionsForTopic(consumer, dlqTopic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
            int count = 0;

            while (hasRemaining(consumer, endOffsets) && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.printf("[%d] partition=%d offset=%d key=%s%n  value=%s%n",
                            count, record.partition(), record.offset(), record.key(), record.value());
                }
            }
            LOG.info("Found {} DLQ records in topic '{}'", count, dlqTopic);
        }
    }

    private static void dryRun(String bootstrap, String topic, long timeoutSeconds) {
        LOG.info("Dry-run on topic '{}'", topic);
        Properties consumerProps = consumerProperties(bootstrap, "durga-dryrun-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = partitionsForTopic(consumer, topic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
            int count = 0;

            while (hasRemaining(consumer, endOffsets) && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.printf("[%d] topic=%s partition=%d offset=%d key=%s%n  value=%s%n",
                            count, topic, record.partition(), record.offset(), record.key(),
                            truncate(record.value(), 500));
                }
            }
            LOG.info("Found {} records in topic '{}'", count, topic);
        }
    }

    private static List<TopicPartition> partitionsForTopic(KafkaConsumer<String, String> consumer, String topic) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
        if (infos == null || infos.isEmpty()) {
            throw new IllegalArgumentException("No partitions found for topic: " + topic);
        }
        return infos.stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
    }

    private static boolean hasRemaining(KafkaConsumer<String, String> consumer,
                                        Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (consumer.position(entry.getKey()) < entry.getValue()) {
                return true;
            }
        }
        return false;
    }

    private static ProcessEvent parseReplayEvent(String value) throws Exception {
        try {
            return ProcessEvent.fromJson(value);
        } catch (IllegalArgumentException directParseFailure) {
            JsonNode node = MAPPER.readTree(value);
            JsonNode originalJson = node.get("originalJson");
            if (originalJson != null && originalJson.isTextual()) {
                return ProcessEvent.fromJson(originalJson.asText());
            }
            JsonNode originalEvent = node.get("originalEvent");
            if (originalEvent != null && originalEvent.isObject()) {
                return MAPPER.treeToValue(originalEvent, ProcessEvent.class);
            }
            throw directParseFailure;
        }
    }

    private static void executeReplay(String bootstrap, String sourceTopic, ReplayPlan plan) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        int replayed = 0;
        int failed = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (ReplayItem item : plan.items()) {
                try {
                    ProcessEvent original = item.event();
                    String replayInstanceId = original.processInstanceId() + "-replay-" + System.currentTimeMillis();
                    String targetTopic = resolveTargetTopic(sourceTopic);
                    String replayReason = "replayed from " + sourceTopic
                            + " offset " + item.offset();
                    ProcessEvent replayedEvent = original
                            .withInstanceId(replayInstanceId)
                            .withReplay(replayReason, sourceTopic + "/" + item.offset());

                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            targetTopic, replayedEvent.processInstanceId(), replayedEvent.toJson());
                    producer.send(record).get();
                    LOG.info("Replayed {} -> {} (offset={}, new instance={})",
                            sourceTopic, targetTopic, item.offset(), replayInstanceId);
                    replayed++;
                } catch (Exception e) {
                    LOG.error("Failed to replay record at offset {}: {}", item.offset(), e.getMessage());
                    failed++;
                }
            }
            producer.flush();
        }

        LOG.info("Replay complete: {} replayed, {} failed, {} total",
                replayed, failed, plan.size());
    }

    private static String resolveTargetTopic(String sourceTopic) {
        if (sourceTopic.endsWith("_dlq")) {
            return sourceTopic.substring(0, sourceTopic.length() - 4) + "_input";
        }
        return sourceTopic;
    }

    private static Properties consumerProperties(String bootstrap, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        return props;
    }

    private static long parseLongArg(String value, String name) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for " + name + ": '" + value + "'");
        }
    }

    private static String resolveArg(String[] args, String flag, String envVar, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(flag) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        if (envVar != null && System.getenv(envVar) != null) {
            return System.getenv(envVar);
        }
        return defaultValue;
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (arg.equals(flag)) {
                return true;
            }
        }
        return false;
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    private static void printUsage() {
        System.err.println("""
                Usage: ReplayTool <command> [options]
                                
                Commands:
                  replay-dlq      <dlq-topic>  [--dry-run]  [--bootstrap <servers>] [--timeout <s>]
                  replay-offset   <topic> <partition> <from-offset> <to-offset>  [--dry-run] [--bootstrap <servers>]
                  inspect-dlq     <dlq-topic>  [--bootstrap <servers>] [--timeout <s>]
                  dry-run         <topic>      [--bootstrap <servers>] [--timeout <s>]
                                
                The --bootstrap flag overrides the KAFKA_BOOTSTRAP_SERVERS env variable (default: localhost:9094).
                Use --dry-run to preview which records would be replayed without executing.
                                
                Examples:
                  ReplayTool replay-dlq invoice_process_validate_data_dlq --dry-run
                  ReplayTool replay-dlq invoice_process_validate_data_dlq
                  ReplayTool replay-offset process-events 0 100 200
                  ReplayTool inspect-dlq invoice_process_enrich_data_dlq
                """);
    }

    record ReplayItem(ProcessEvent event, String topic, int partition, long offset,
                      String key, String rawValue) {
    }

    static final class ReplayPlan {
        private final String source;
        private final List<ReplayItem> items = new ArrayList<>();

        ReplayPlan(String source) {
            this.source = source;
        }

        void add(ReplayItem item) {
            items.add(item);
        }

        int size() {
            return items.size();
        }

        boolean isEmpty() {
            return items.isEmpty();
        }

        List<ReplayItem> items() {
            return List.copyOf(items);
        }

        void print(java.io.PrintStream out) {
            out.println("=== Replay Plan: " + source + " ===");
            out.println("Records to replay: " + items.size());
            out.println();
            for (int i = 0; i < items.size(); i++) {
                ReplayItem item = items.get(i);
                ProcessEvent e = item.event();
                out.printf("[%d] instance=%s activity=%s status=%s offset=%d topic=%s%n",
                        i + 1,
                        e.processInstanceId(),
                        e.activityId(),
                        e.status(),
                        item.offset(),
                        item.topic());
            }
            out.println("=== End Plan ===");
        }
    }
}
