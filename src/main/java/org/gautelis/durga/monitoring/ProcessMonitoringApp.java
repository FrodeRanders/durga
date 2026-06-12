package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Command-line entry point for the monitoring topology and embedded query server.
 */
public final class ProcessMonitoringApp {
    private ProcessMonitoringApp() {
    }

    /**
     * Starts the Kafka Streams monitoring topology and the embedded HTTP server.
     *
     * @param args optional {@code <bootstrapServers> <applicationId> <httpPort>}
     */
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : bootstrapServersDefault();
        String applicationId = args.length > 1 ? args[1] : "durga-monitoring";
        int httpPort = args.length > 2 ? Integer.parseInt(args[2]) : 8081;
        ProcessMonitoringTopology.MonitoringTopics topics = ProcessMonitoringTopology.MonitoringTopics.defaults();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-state");

        KafkaStreams streams = new KafkaStreams(ProcessMonitoringTopology.buildTopology(topics), props);
        try {
            ProcessMonitoringHttpServer httpServer = new ProcessMonitoringHttpServer(streams, topics, httpPort);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                httpServer.close();
                streams.close();
            }));
            streams.start();
            httpServer.start();
            System.out.println("Process monitoring HTTP server listening on http://localhost:" + httpPort);
        } catch (Exception e) {
            streams.close();
            throw new IllegalStateException("Failed to start process monitoring HTTP server", e);
        }
    }

    private static String bootstrapServersDefault() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9094");
    }
}
