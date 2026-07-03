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
 * Publishes full-process lifecycle events directly to the process-events topic,
 * simulating the complete BPMN execution with correct XOR gateway routing.
 * The monitoring topology picks these up to build state, latency, and counts.
 */
public final class E2ELifecyclePublisher {

    private static final List<String> CUSTOMER_EMAILS = List.of(
            "alice@example.com", "bob@example.com", "carol@example.com",
            "dave@example.com", "eve@example.com", "frank@example.com"
    );
    private static final List<String> STATUSES = List.of("pending", "shipped", "delivered", "cancelled", "returned");
    private static final List<String> CUSTOMER_NAMES = List.of(
            "Alice Alpha", "Bob Bravo", "Carol Charlie", "Dave Delta", "Eve Echo", "Frank Foxtrot"
    );
    private static final Random RNG = new Random();

    // e2e_pipeline activities in order
    private static final List<String> COMMON = List.of("transform_order", "coerce_types");
    private static final List<String> HIGH   = List.of("route_decision", "enrich_high_value", "validate_high_value", "aggregate_high_value");
    private static final List<String> LOW    = List.of("aggregate_low_value");

    public static void main(String[] args) {
        String bootstrap  = args.length > 0 ? args[0] : "localhost:9094";
        String processId  = args.length > 1 ? args[1] : "e2e_pipeline";
        long   intervalMs = Long.parseLong(args.length > 2 ? args[2] : "100");
        long   maxCount   = Long.parseLong(args.length > 3 ? args[3] : "-1");

        String eventsTopic = "process-events-" + processId;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
            System.out.println("Lifecycle feed: " + eventsTopic + " (interval=" + intervalMs + "ms" + (maxCount > 0 ? ", count=" + maxCount : ", continuous") + ")");

            long n = 0;
            while (maxCount < 0 || n < maxCount) {
                String iid   = UUID.randomUUID().toString();
                String corr  = UUID.randomUUID().toString();

                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("id", n + 1);
                payload.put("customer", Map.of("name", CUSTOMER_NAMES.get(RNG.nextInt(CUSTOMER_NAMES.size())),
                                               "email", CUSTOMER_EMAILS.get(RNG.nextInt(CUSTOMER_EMAILS.size()))));
                payload.put("items", 1 + RNG.nextInt(5));
                double total = Math.round((50.0 + RNG.nextDouble() * 950.0) * 100.0) / 100.0;
                payload.put("total", total);
                payload.put("status", STATUSES.get(RNG.nextInt(STATUSES.size())));
                payload.put("created_at", System.currentTimeMillis());

                // PROCESS_STARTED
                send(p, eventsTopic, iid, new ProcessEvent(iid, processId, "start", UUID.randomUUID().toString(),
                        corr, payload, ProcessEvent.Status.STARTED, null, ProcessEvent.EventType.PROCESS_STARTED, "v1", null, null));

                // Common path
                for (String aid : COMMON) {
                    sleep(20, 80);
                    send(p, eventsTopic, iid, new ProcessEvent(iid, processId, aid, UUID.randomUUID().toString(),
                            corr, payload, null, null, ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", null, null));
                    sleep(20, 80);
                    send(p, eventsTopic, iid, new ProcessEvent(iid, processId, aid, UUID.randomUUID().toString(),
                            corr, payload, ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", null, null));
                }

                // XOR gateway — pick branch based on total
                List<String> branch = total > 500.0 ? HIGH : LOW;
                for (String aid : branch) {
                    sleep(20, 80);
                    send(p, eventsTopic, iid, new ProcessEvent(iid, processId, aid, UUID.randomUUID().toString(),
                            corr, payload, null, null, ProcessEvent.EventType.ACTIVITY_ENTERED, "v1", null, null));
                    sleep(20, 80);
                    send(p, eventsTopic, iid, new ProcessEvent(iid, processId, aid, UUID.randomUUID().toString(),
                            corr, payload, ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.ACTIVITY_COMPLETED, "v1", null, null));
                }

                // PROCESS_COMPLETED
                send(p, eventsTopic, iid, new ProcessEvent(iid, processId, "completed", UUID.randomUUID().toString(),
                        corr, payload, ProcessEvent.Status.COMPLETED, null, ProcessEvent.EventType.PROCESS_COMPLETED, "v1", null, null));

                System.out.println("Instance " + iid.substring(0,8) + " total=" + total + " branch=" + (total > 500 ? "high" : "low"));
                n++;
                if (maxCount < 0 || n < maxCount) {
                    try { Thread.sleep(intervalMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                }
            }
            p.flush();
        }
    }

    private static void send(KafkaProducer<String, String> p, String topic, String key, ProcessEvent e) {
        p.send(new ProducerRecord<>(topic, key, e.toJson()));
    }

    private static void sleep(int min, int max) {
        try { Thread.sleep(min + (long)(Math.random() * (max - min))); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
