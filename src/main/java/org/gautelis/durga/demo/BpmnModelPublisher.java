package org.gautelis.durga.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Publishes a BPMN 2.0 model to the {@code process-models} Kafka topic so the
 * monitoring server can cache and serve it for the diagram viewer.
 * <p>
 * Each process (fat jar) should call this on startup to register its model.
 * Models are keyed by {@code processId} — sending a new model overwrites the
 * previous one (latest-wins semantics).
 * <p>
 * Usage:
 * <pre>{@code
 *   java -cp app.jar org.gautelis.durga.demo.BpmnModelPublisher \
 *       localhost:9094 invoice_receipt path/to/invoice_receipt.bpmn
 * }</pre>
 */
public final class BpmnModelPublisher {

    private static final String TOPIC = "process-models";

    private BpmnModelPublisher() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: BpmnModelPublisher <bootstrapServers> <processId> <bpmnPath>");
            System.exit(1);
        }
        String bootstrap = args[0];
        String processId = args[1];
        Path bpmnPath = Path.of(args[2]);

        if (!Files.isRegularFile(bpmnPath)) {
            System.err.println("BPMN file not found: " + bpmnPath);
            System.exit(1);
        }

        String xml = Files.readString(bpmnPath, StandardCharsets.UTF_8);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, processId, xml));
            producer.flush();
            System.out.println("Published BPMN model for process '" + processId
                    + "' to topic " + TOPIC + " (" + xml.length() + " bytes)");
        }
    }
}
