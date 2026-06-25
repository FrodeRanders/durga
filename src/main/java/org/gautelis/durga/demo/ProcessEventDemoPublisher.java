package org.gautelis.durga.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gautelis.durga.ProcessEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Publishes a simple synthetic lifecycle directly to the canonical {@code process-events} topic.
 */
public final class ProcessEventDemoPublisher {
    private ProcessEventDemoPublisher() {
    }

    /**
     * Publishes a happy-path lifecycle for quick monitoring demos.
     *
     * @param args optional {@code <bootstrapServers> <processId> <activity1,activity2,...> <businessKey>}
     */
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : bootstrapServersDefault();
        String processId = args.length > 1 ? args[1] : "invoice_receipt";
        String activitiesArg = args.length > 2 ? args[2] : "register_invoice,review_invoice,notify_requester";
        String businessKey = args.length > 3 ? args[3] : "demo-" + UUID.randomUUID();
        String topic = topicForProcess(processId);

        List<String> activities = parseActivities(activitiesArg);
        String processInstanceId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", "demo-publisher");
        payload.put("valid", true);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            publish(producer, topic, processInstanceId, new ProcessEvent(
                    processInstanceId,
                    processId,
                    "start",
                    UUID.randomUUID().toString(),
                    correlationId,
                    payload,
                    ProcessEvent.Status.STARTED,
                    null,
                    ProcessEvent.EventType.PROCESS_STARTED,
                    "v1",
                    businessKey,
                    null
            ));

            for (String activity : activities) {
                publish(producer, topic, processInstanceId, new ProcessEvent(
                        processInstanceId,
                        processId,
                        activity,
                        UUID.randomUUID().toString(),
                        correlationId,
                        payload,
                        ProcessEvent.Status.STARTED,
                        null,
                        ProcessEvent.EventType.ACTIVITY_ENTERED,
                        "v1",
                        businessKey,
                        null
                ));
                publish(producer, topic, processInstanceId, new ProcessEvent(
                        processInstanceId,
                        processId,
                        activity,
                        UUID.randomUUID().toString(),
                        correlationId,
                        payload,
                        ProcessEvent.Status.COMPLETED,
                        null,
                        ProcessEvent.EventType.ACTIVITY_COMPLETED,
                        "v1",
                        businessKey,
                        null
                ));
            }

            publish(producer, topic, processInstanceId, new ProcessEvent(
                    processInstanceId,
                    processId,
                    "completed",
                    UUID.randomUUID().toString(),
                    correlationId,
                    payload,
                    ProcessEvent.Status.COMPLETED,
                    null,
                    ProcessEvent.EventType.PROCESS_COMPLETED,
                    "v1",
                    businessKey,
                    null
            ));

            producer.flush();
        }

        System.out.println("Published demo lifecycle to " + topic + " for instanceId=" + processInstanceId);
    }

    private static void publish(KafkaProducer<String, String> producer, String topic, String key, ProcessEvent event) {
        producer.send(new ProducerRecord<>(topic, key, event.toJson()));
    }

    private static String topicForProcess(String processId) {
        return "process-events-" + processId;
    }

    private static List<String> parseActivities(String activitiesArg) {
        List<String> activities = new ArrayList<>();
        for (String activity : activitiesArg.split(",")) {
            String normalized = activity.trim();
            if (!normalized.isEmpty()) {
                activities.add(normalized);
            }
        }
        if (activities.isEmpty()) {
            throw new IllegalArgumentException("At least one activity must be provided");
        }
        return activities;
    }

    private static String bootstrapServersDefault() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9094");
    }
}
