package org.gautelis.durga.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gautelis.durga.ProcessEvent;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Continuously publishes lifecycle events to exercise the monitoring topology.
 */
public final class ContinuousFeedPublisher {

    private ContinuousFeedPublisher() {
    }

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
        String processId = args.length > 1 ? args[1]
                : System.getenv().getOrDefault("FEED_PROCESS_ID", "invoice_receipt");
        long intervalMs = Long.parseLong(args.length > 2 ? args[2]
                : System.getenv().getOrDefault("FEED_INTERVAL_MS", "1000"));
        String topic = topicForProcess(processId);

        List<String> activities = List.of("register_invoice", "review_invoice", "notify_requester");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Continuous feed started: process=" + processId
                    + ", interval=" + intervalMs + "ms, bootstrap=" + bootstrap);

            //noinspection InfiniteLoopStatement
            while (true) {
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
                System.out.println("Published lifecycle for instanceId=" + instanceId);

                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void publish(KafkaProducer<String, String> producer, String topic, String key, ProcessEvent event) {
        producer.send(new ProducerRecord<>(topic, key, event.toJson()));
    }

    private static String topicForProcess(String processId) {
        return "process-events-" + processId;
    }
}
