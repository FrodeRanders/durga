package org.gautelis.durga;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Kafka integration tests.
 * <p>
 * Uses Testcontainers 2.0.2 (which auto-detects the Docker socket on all
 * platforms) with a GenericContainer for Kafka in KRaft mode.
 * Falls back to socket probing if DOCKER_HOST is not set.
 */
public abstract class KafkaIntegrationTestBase {
    private static final int KAFKA_PORT = 9092;

    protected static GenericContainer<?> kafka;
    protected static String bootstrapServers;

    static {
        configureDockerHost();
    }

    @BeforeClass
    public static void startKafka() {
        try {
            DockerClientFactory.instance().client();
        } catch (Exception e) {
            System.err.println("Docker not available, skipping integration tests.");
            System.err.println("See doc/testcontainers-setup.md for setup instructions.");
            org.junit.Assume.assumeTrue("Docker not available", false);
            return;
        }

        int port = KAFKA_PORT;
        int hostPort = port + 10000;
        String containerHost = envOr("DURGA_DOCKER_HOST_IP", "localhost"); // fixed host port to avoid random mapping

        kafka = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_LISTENERS", "BROKER://0.0.0.0:" + port + ",CONTROLLER://0.0.0.0:9093")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://" + containerHost + ":" + hostPort)
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
                .withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withPortBindings(new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindPort(hostPort),
                                new com.github.dockerjava.api.model.ExposedPort(port))))
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(90)));
        kafka.start();

        bootstrapServers = containerHost + ":" + hostPort;
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    protected static String bootstrapServers() {
        return bootstrapServers;
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : fallback;
    }

    private static void configureDockerHost() {
        if (notBlank(System.getenv("DOCKER_HOST"))) return;
        if (notBlank(System.getProperty("DOCKER_HOST"))) return;
        String socket = findDockerSocket();
        if (socket != null) {
            System.setProperty("DOCKER_HOST", "unix://" + socket);
        }
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String findDockerSocket() {
        List<String> candidates = new ArrayList<>();
        addIfSocket(candidates, "/var/run/docker.sock");
        String home = System.getProperty("user.home");
        if (home != null) {
            addIfSocket(candidates, home + "/.docker/run/docker.sock");
            addIfSocket(candidates, home + "/Library/Containers/com.docker.docker/Data/docker.raw.sock");
        }
        addIfSocket(candidates, "/run/docker.sock");
        for (String c : candidates) {
            if (canConnect(c)) return c;
        }
        return null;
    }

    private static void addIfSocket(List<String> candidates, String path) {
        if (Files.exists(Path.of(path))) candidates.add(path);
    }

    static boolean canConnect(String socketPath) {
        try {
            var addr = UnixDomainSocketAddress.of(Path.of(socketPath));
            try (var channel = SocketChannel.open(addr)) {
                return channel.isConnected();
            }
        } catch (IOException e) {
            return false;
        }
    }
}
