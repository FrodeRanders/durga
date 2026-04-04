package org.gautelis.durga.monitoring;

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

public final class ProcessEventScenarioRunner {
    private static final String TOPIC = "process-events";

    private ProcessEventScenarioRunner() {
    }

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9094";
        String scenario = args.length > 1 ? args[1] : "happy";
        String processId = args.length > 2 ? args[2] : "invoice_receipt";
        String activitiesArg = args.length > 3 ? args[3] : "register_invoice,review_invoice,notify_requester";
        String businessKey = args.length > 4 ? args[4] : scenario + "-" + UUID.randomUUID();

        List<String> activities = parseActivities(activitiesArg);
        String processInstanceId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", "scenario-runner");
        payload.put("scenario", scenario);
        payload.put("valid", true);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, "start", correlationId, payload,
                    ProcessEvent.Status.STARTED, null, ProcessEvent.EventType.PROCESS_STARTED, businessKey
            ));

            switch (scenario) {
                case "happy" -> runHappyPath(producer, processInstanceId, processId, correlationId, payload, activities, businessKey);
                case "stuck" -> runStuckPath(producer, processInstanceId, processId, correlationId, payload, activities, businessKey);
                case "failed" -> runFailedPath(producer, processInstanceId, processId, correlationId, payload, activities, businessKey);
                default -> throw new IllegalArgumentException("Unknown scenario: " + scenario + ". Use happy, stuck, or failed.");
            }

            producer.flush();
        }

        System.out.println("Published scenario=" + scenario + " to " + TOPIC + " for instanceId=" + processInstanceId);
    }

    private static void runHappyPath(
            KafkaProducer<String, String> producer,
            String processInstanceId,
            String processId,
            String correlationId,
            Map<String, Object> payload,
            List<String> activities,
            String businessKey
    ) {
        for (String activity : activities) {
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, activity, correlationId, payload,
                    ProcessEvent.Status.STARTED, null, ProcessEvent.EventType.ACTIVITY_ENTERED, businessKey
            ));
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, activity, correlationId, payload,
                    ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.ACTIVITY_COMPLETED, businessKey
            ));
        }
        publish(producer, processInstanceId, event(
                processInstanceId, processId, "completed", correlationId, payload,
                ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.PROCESS_COMPLETED, businessKey
        ));
    }

    private static void runStuckPath(
            KafkaProducer<String, String> producer,
            String processInstanceId,
            String processId,
            String correlationId,
            Map<String, Object> payload,
            List<String> activities,
            String businessKey
    ) {
        for (int i = 0; i < activities.size(); i++) {
            String activity = activities.get(i);
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, activity, correlationId, payload,
                    ProcessEvent.Status.STARTED, null, ProcessEvent.EventType.ACTIVITY_ENTERED, businessKey
            ));
            if (i < activities.size() - 1) {
                publish(producer, processInstanceId, event(
                        processInstanceId, processId, activity, correlationId, payload,
                        ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.ACTIVITY_COMPLETED, businessKey
                ));
            }
        }
    }

    private static void runFailedPath(
            KafkaProducer<String, String> producer,
            String processInstanceId,
            String processId,
            String correlationId,
            Map<String, Object> payload,
            List<String> activities,
            String businessKey
    ) {
        String failingActivity = activities.getLast();
        for (String activity : activities) {
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, activity, correlationId, payload,
                    ProcessEvent.Status.STARTED, null, ProcessEvent.EventType.ACTIVITY_ENTERED, businessKey
            ));
            if (activity.equals(failingActivity)) {
                publish(producer, processInstanceId, event(
                        processInstanceId,
                        processId,
                        activity,
                        correlationId,
                        payload,
                        ProcessEvent.Status.FAILED,
                        new ProcessEvent.ErrorInfo("Demo failure", "DEMO_FAILED"),
                        ProcessEvent.EventType.PROCESS_FAILED,
                        businessKey
                ));
                return;
            }
            publish(producer, processInstanceId, event(
                    processInstanceId, processId, activity, correlationId, payload,
                    ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.ACTIVITY_COMPLETED, businessKey
            ));
        }
    }

    private static ProcessEvent event(
            String processInstanceId,
            String processId,
            String activityId,
            String correlationId,
            Map<String, Object> payload,
            ProcessEvent.Status status,
            ProcessEvent.ErrorInfo error,
            ProcessEvent.EventType eventType,
            String businessKey
    ) {
        return new ProcessEvent(
                processInstanceId,
                processId,
                activityId,
                UUID.randomUUID().toString(),
                correlationId,
                payload,
                status,
                error,
                eventType,
                "v1",
                businessKey,
                null
        );
    }

    private static void publish(KafkaProducer<String, String> producer, String key, ProcessEvent event) {
        producer.send(new ProducerRecord<>(TOPIC, key, event.toJson()));
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
}
