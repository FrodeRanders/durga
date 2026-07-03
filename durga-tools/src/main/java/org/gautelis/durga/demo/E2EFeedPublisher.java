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
import java.util.Random;
import java.util.UUID;

/**
 * Publishes ProcessEvent PROCESS_STARTED records directly to a task's input channel,
 * simulating real orders entering the pipeline. The generated process workers pick
 * these up and execute through the full BPMN pipeline (plugins, gateways, etc.).
 *
 * Usage: java E2EFeedPublisher localhost:9094 e2e_pipeline 100 [count]
 */
public final class E2EFeedPublisher {

    private static final List<String> CUSTOMER_EMAILS = List.of(
            "alice@example.com", "bob@example.com", "carol@example.com",
            "dave@example.com", "eve@example.com", "frank@example.com"
    );
    private static final List<String> STATUSES = List.of(
            "pending", "shipped", "delivered", "cancelled", "returned"
    );
    private static final List<String> CUSTOMER_NAMES = List.of(
            "Alice Alpha", "Bob Bravo", "Carol Charlie",
            "Dave Delta", "Eve Echo", "Frank Foxtrot"
    );
    private static final Random RNG = new Random();

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9094";
        String processId = args.length > 1 ? args[1] : "e2e_pipeline";
        long intervalMs = Long.parseLong(args.length > 2 ? args[2] : "100");
        long maxCount = Long.parseLong(args.length > 3 ? args[3] : "-1");
        String targetTopic = processId + "_start";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("E2E feed: " + targetTopic
                    + " (bootstrap=" + bootstrap + ", interval=" + intervalMs + "ms"
                    + (maxCount > 0 ? ", count=" + maxCount : ", continuous") + ")");

            long published = 0;
            while (maxCount < 0 || published < maxCount) {
                long idx = published + 1;
                String instanceId = UUID.randomUUID().toString();
                String corrId = UUID.randomUUID().toString();
                String tokenId = UUID.randomUUID().toString();

                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("id", idx);
                payload.put("customer", Map.of(
                        "name", CUSTOMER_NAMES.get(RNG.nextInt(CUSTOMER_NAMES.size())),
                        "email", CUSTOMER_EMAILS.get(RNG.nextInt(CUSTOMER_EMAILS.size()))
                ));
                payload.put("items", 1 + RNG.nextInt(5));
                payload.put("total", Math.round((50.0 + RNG.nextDouble() * 950.0) * 100.0) / 100.0);
                payload.put("status", STATUSES.get(RNG.nextInt(STATUSES.size())));
                payload.put("created_at", System.currentTimeMillis());

                ProcessEvent event = new ProcessEvent(
                        instanceId, processId, "start", tokenId, corrId,
                        payload, ProcessEvent.Status.STARTED, null,
                        ProcessEvent.EventType.PROCESS_STARTED, "v1", null, null);

                producer.send(new ProducerRecord<>(targetTopic, instanceId, event.toJson()));
                System.out.println("Sent instance=" + instanceId + " total=" + payload.get("total"));

                published++;
                if (maxCount < 0 || published < maxCount) {
                    try {
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            producer.flush();
        }
    }
}
