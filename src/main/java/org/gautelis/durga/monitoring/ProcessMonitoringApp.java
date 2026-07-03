package org.gautelis.durga.monitoring;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Quarkus entry point for the monitoring topology and REST API.
 * Monitors all Durga processes across the organisation — always multi-process.
 */
@QuarkusMain
public final class ProcessMonitoringApp {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitoringApp.class);

    static String bootstrapServers;
    static String applicationId;

    private ProcessMonitoringApp() {
    }

    public static void main(String[] args) {
        LOG.info("Starting ProcessMonitoringApp");
        try {
            bootstrapServers = args.length > 0 ? args[0] : bootstrapServersDefault();
            applicationId = args.length > 1 ? args[1] : "durga-monitoring";
            Quarkus.run(args);
            LOG.info("ProcessMonitoringApp completed successfully");
        } catch (Exception e) {
            LOG.error("ProcessMonitoringApp failed", e);
            throw e;
        }
    }

    @Produces
    @Singleton
    MonitoringState monitoringState() {
        ProcessMonitoringTopology.MonitoringTopics topics =
                ProcessMonitoringTopology.MonitoringTopics.forAllProcesses();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                System.getProperty("durga.streams.commit.interval.ms", "1000"));
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("durga.streams.state.dir", "target/kafka-streams-state"));

        KafkaStreams streams = new KafkaStreams(
                ProcessMonitoringTopology.buildTopology(topics), props);
        Properties faultProps = new Properties();
        faultProps.putAll(props);
        faultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-fault-detection");
        faultProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        faultProps.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("durga.fault.streams.state.dir", "target/kafka-streams-fault-state"));
        KafkaStreams faultStreams = new KafkaStreams(
                FaultDetectionTopology.buildTopology(
                        Set.of(),
                        FaultDetectionTopology.DEFAULT_EVENTS_PATTERN,
                        FaultDetectionTopology.DEFAULT_ALARMS_TOPIC,
                        topics.modelsTopic()),
                faultProps);
        Properties alarmStateProps = new Properties();
        alarmStateProps.putAll(props);
        alarmStateProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-alarm-state");
        alarmStateProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        alarmStateProps.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("durga.alarm.streams.state.dir", "target/kafka-streams-alarm-state"));
        KafkaStreams alarmStateStreams = new KafkaStreams(
                AlarmStateTopology.buildTopology(
                        FaultDetectionTopology.DEFAULT_ALARMS_TOPIC,
                        AlarmStateTopology.DEFAULT_ALARM_STATE_STORE),
                alarmStateProps);
        ProcessMonitoringQueryService queryService =
                new ProcessMonitoringQueryService(streams, topics);
        AlarmStateQueryService alarmQueryService =
                new AlarmStateQueryService(alarmStateStreams, AlarmStateTopology.DEFAULT_ALARM_STATE_STORE);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(30));
            faultStreams.close(Duration.ofSeconds(30));
            alarmStateStreams.close(Duration.ofSeconds(30));
        }));

        createTopicsIfNeeded(bootstrapServers, topics);

        streams.start();
        faultStreams.start();
        alarmStateStreams.start();
        LOG.info("Monitoring topology started (state dir: {})", props.getProperty(StreamsConfig.STATE_DIR_CONFIG));
        LOG.info("Fault detection topology started (state dir: {})", faultProps.getProperty(StreamsConfig.STATE_DIR_CONFIG));
        LOG.info("Alarm state topology started (state dir: {})", alarmStateProps.getProperty(StreamsConfig.STATE_DIR_CONFIG));

        return new MonitoringState(streams, faultStreams, alarmStateStreams, queryService, alarmQueryService);
    }

    private static void createTopicsIfNeeded(String bootstrapServers, ProcessMonitoringTopology.MonitoringTopics topics) {
        List<String> allTopics = List.of(
                topics.stateTopic(),
                topics.countsTopic(),
                topics.activeTopic(),
                topics.latencyTopic(),
                topics.trendsTopic(),
                topics.modelsTopic(),
                FaultDetectionTopology.DEFAULT_ALARMS_TOPIC);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (var admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : allTopics) {
                if (!existing.contains(t)) {
                    toCreate.add(new NewTopic(t, 2, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get();
                LOG.info("Created topics: {}", toCreate.stream().map(NewTopic::name).toList());
            }
        } catch (Exception e) {
            LOG.warn("Could not auto-create Kafka topics (broker may not be reachable yet): {}", e.getMessage());
        }
    }

    /** Injectable state holder for the REST resource. */
    public record MonitoringState(
            KafkaStreams streams,
            KafkaStreams faultStreams,
            KafkaStreams alarmStateStreams,
            ProcessMonitoringQueryService queryService,
            AlarmStateQueryService alarmQueryService
    ) {
    }

    private static String bootstrapServersDefault() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9094");
    }
}
