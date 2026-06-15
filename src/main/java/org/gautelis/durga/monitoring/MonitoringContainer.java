package org.gautelis.durga.monitoring;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;

/**
 * Standalone entry point for container deployments.
 * Starts Kafka Streams and the embedded HTTP server directly,
 * without Quarkus dependencies.
 */
public final class MonitoringContainer {
    private MonitoringContainer() {
    }

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : bootstrapServersDefault();
        String appId = args.length > 1 ? args[1] : "durga-monitoring";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 8081;
        String processId = args.length > 3 && !args[3].isBlank() ? args[3] : defaultProcessId();

        var topics = ProcessMonitoringTopology.MonitoringTopics.forProcess(processId);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("durga.streams.state.dir", "/tmp/kafka-streams-state"));

        KafkaStreams streams = new KafkaStreams(
                ProcessMonitoringTopology.buildTopology(topics), props);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (var admin = AdminClient.create(adminProps)) {
            for (String topic : List.of(
                    topics.stateTopic(),
                    topics.countsTopic(),
                    topics.activeTopic(),
                    topics.latencyTopic(),
                    topics.trendsTopic())) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)));
            }
        }

        try {
            var httpServer = new ProcessMonitoringHttpServer(streams, topics, port);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                httpServer.close();
                streams.close();
            }));
            streams.start();
            httpServer.start();
            System.out.println("Monitoring listening on http://0.0.0.0:" + port);
        } catch (Exception e) {
            streams.close();
            throw new IllegalStateException("Failed to start monitoring", e);
        }
    }

    private static String bootstrapServersDefault() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9094");
    }

    private static String defaultProcessId() {
        return System.getProperty("durga.monitoring.process.id", "default");
    }
}
