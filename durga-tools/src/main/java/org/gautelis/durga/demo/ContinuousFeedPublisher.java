package org.gautelis.durga.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.Activity;
import org.gautelis.durga.ProcessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Continuously publishes lifecycle events to exercise the monitoring topology.
 * Reads the BPMN model to extract actual activity IDs, making it generic across
 * any process definition.
 */
public final class ContinuousFeedPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFeedPublisher.class);

    private ContinuousFeedPublisher() {
    }

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
        String processId = args.length > 1 ? args[1]
                : System.getenv().getOrDefault("FEED_PROCESS_ID", "invoice_receipt");
        long intervalMs = Long.parseLong(args.length > 2 ? args[2]
                : System.getenv().getOrDefault("FEED_INTERVAL_MS", "1000"));
        String bpmnDir = args.length > 3 ? args[3]
                : System.getenv().getOrDefault("BPMN_DIR", "durga-tools/src/test/resources/bpmn");
        long maxCount = Long.parseLong(args.length > 4 ? args[4]
                : System.getenv().getOrDefault("FEED_COUNT", "-1"));

        List<String> activities = resolveActivities(processId, bpmnDir);
        if (activities.isEmpty()) {
            LOG.error("No activities found for process '{}' in {}", processId, bpmnDir);
            System.exit(1);
        }

        String topic = topicForProcess(processId);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Continuous feed started: process=" + processId
                    + ", interval=" + intervalMs + "ms, bootstrap=" + bootstrap
                    + ", count=" + (maxCount < 0 ? "continuous" : maxCount)
                    + ", activities=" + activities);

            long published = 0;
            while (maxCount < 0 || published < maxCount) {
                String instanceId = UUID.randomUUID().toString();
                String corrId = UUID.randomUUID().toString();
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("source", "continuous-feed");

                publish(producer, topic, instanceId, new ProcessEvent(
                        instanceId, processId, "start", UUID.randomUUID().toString(),
                        corrId, payload, ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", null, null));

                for (String activity : activities) {
                    publish(producer, topic, instanceId, new ProcessEvent(
                            instanceId, processId, activity, UUID.randomUUID().toString(),
                            corrId, payload, null, null,
                            ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", null, null));

                    long workTime = 50 + (long) (Math.random() * 200);
                    try {
                        Thread.sleep(workTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    publish(producer, topic, instanceId, new ProcessEvent(
                            instanceId, processId, activity, UUID.randomUUID().toString(),
                            corrId, payload, ProcessEvent.Status.COMPLETED, null,
                            ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", null, null));
                }

                publish(producer, topic, instanceId, new ProcessEvent(
                        instanceId, processId, "completed", UUID.randomUUID().toString(),
                        corrId, payload, ProcessEvent.Status.COMPLETED, null,
                        ProcessEvent.EventType.PROCESS_COMPLETED, "v1", null, null));

                producer.flush();
                published++;
                System.out.println("Published lifecycle for instanceId=" + instanceId);

                if (maxCount >= 0 && published >= maxCount) {
                    break;
                }
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static List<String> resolveActivities(String processId, String bpmnDir) {
        Path bpmnFile = resolveBpmnFile(processId, bpmnDir);
        if (!Files.isRegularFile(bpmnFile)) {
            LOG.warn("BPMN file not found: {}", bpmnFile);
            return List.of();
        }
        try {
            BpmnModelInstance model = Bpmn.readModelFromFile(bpmnFile.toFile());
            return model.getModelElementsByType(Activity.class).stream()
                    .map(Activity::getId)
                    .filter(id -> id != null && !id.isBlank())
                    .distinct()
                    .toList();
        } catch (Exception e) {
            LOG.error("Failed to read BPMN model from {}", bpmnFile, e);
            return List.of();
        }
    }

    private static Path resolveBpmnFile(String processId, String bpmnDir) {
        Path direct = Path.of(bpmnDir, processId + ".bpmn");
        if (Files.isRegularFile(direct)) {
            return direct;
        }
        String alternateId = processId.contains("-")
                ? processId.replace('-', '_')
                : processId.replace('_', '-');
        Path alternate = Path.of(bpmnDir, alternateId + ".bpmn");
        return Files.isRegularFile(alternate) ? alternate : direct;
    }

    private static void publish(KafkaProducer<String, String> producer, String topic, String key, ProcessEvent event) {
        producer.send(new ProducerRecord<>(topic, key, event.toJson()));
    }

    private static String topicForProcess(String processId) {
        return "process-events-" + processId;
    }
}
