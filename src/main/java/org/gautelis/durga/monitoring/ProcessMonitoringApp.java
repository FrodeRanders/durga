package org.gautelis.durga.monitoring;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Quarkus entry point for the monitoring topology and REST API.
 */
@QuarkusMain
public final class ProcessMonitoringApp {

    static String bootstrapServers;
    static String applicationId;

    private ProcessMonitoringApp() {
    }

    public static void main(String[] args) {
        bootstrapServers = args.length > 0 ? args[0] : bootstrapServersDefault();
        applicationId = args.length > 1 ? args[1] : "durga-monitoring";
        Quarkus.run(args);
    }

    @Produces
    @Singleton
    MonitoringState monitoringState() {
        ProcessMonitoringTopology.MonitoringTopics topics =
                ProcessMonitoringTopology.MonitoringTopics.defaults();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("durga.streams.state.dir", "target/kafka-streams-state"));

        KafkaStreams streams = new KafkaStreams(
                ProcessMonitoringTopology.buildTopology(topics), props);
        ProcessMonitoringQueryService queryService =
                new ProcessMonitoringQueryService(streams, topics);

        streams.start();
        System.out.println("Monitoring topology started (state dir: "
                + props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + ")");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));

        return new MonitoringState(streams, queryService);
    }

    /** Injectable state holder for the REST resource. */
    public record MonitoringState(
            KafkaStreams streams,
            ProcessMonitoringQueryService queryService
    ) {
    }

    private static String bootstrapServersDefault() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9094");
    }
}
