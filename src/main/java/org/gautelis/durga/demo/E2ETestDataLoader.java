package org.gautelis.durga.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Loads varied test order data into an E2E pipeline input topic.
 * <p>
 * Usage (bounded):
 * <pre>java E2ETestDataLoader --bootstrap-servers localhost:9094 --topic e2e_pipeline_start --count 1000 --interval-ms 100</pre>
 * <p>
 * Usage (continuous, Ctrl-C to stop):
 * <pre>java E2ETestDataLoader --bootstrap-servers localhost:9094 --topic e2e_pipeline_start</pre>
 */
public class E2ETestDataLoader {
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9094";
        String topic = "e2e_pipeline_start";
        long maxCount = -1;
        long intervalMs = 100;
        boolean continuous = true;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers":
                    bootstrapServers = args[++i];
                    break;
                case "--topic":
                    topic = args[++i];
                    break;
                case "--count":
                    maxCount = Long.parseLong(args[++i]);
                    continuous = false;
                    break;
                case "--interval-ms":
                    intervalMs = Long.parseLong(args[++i]);
                    break;
                case "--continuous":
                    continuous = true;
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.err.println("Usage: E2ETestDataLoader [--bootstrap-servers HOST:PORT] [--topic NAME] [--count N] [--interval-ms MS] [--continuous]");
                    System.exit(1);
            }
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Loading test data into " + topic + " (bootstrap: " + bootstrapServers + ")");
            if (continuous && maxCount < 0) {
                System.out.println("Continuous mode - press Ctrl-C to stop");
            } else {
                System.out.println("Bounded mode - will publish " + maxCount + " records");
            }

            long count = 0;
            while (continuous && maxCount < 0 || count < maxCount) {
                String key = "order-" + count;
                String payload = generateOrder(count);
                producer.send(new ProducerRecord<>(topic, key, payload));
                count++;
                if (count % 100 == 0) {
                    producer.flush();
                    System.out.println("Published " + count + " records");
                }
                if (count % 10 == 0) {
                    Thread.sleep(intervalMs);
                }
            }
            producer.flush();
            System.out.println("Done. Published " + count + " records total.");
        }
    }

    static String generateOrder(long index) throws Exception {
        String status = STATUSES.get(RNG.nextInt(STATUSES.size()));
        String email = CUSTOMER_EMAILS.get(RNG.nextInt(CUSTOMER_EMAILS.size()));
        String name = CUSTOMER_NAMES.get(RNG.nextInt(CUSTOMER_NAMES.size()));
        int itemCount = 1 + RNG.nextInt(5);
        double amount = 50.0 + RNG.nextDouble() * 950.0;

        Map<String, Object> order = Map.of(
                "id", index,
                "customer", Map.of(
                        "name", name,
                        "email", email
                ),
                "items", itemCount,
                "total", Math.round(amount * 100.0) / 100.0,
                "status", status,
                "created_at", Instant.now().toEpochMilli()
        );
        return MAPPER.writeValueAsString(order);
    }
}
