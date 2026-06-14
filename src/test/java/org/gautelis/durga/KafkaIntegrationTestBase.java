package org.gautelis.durga;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Kafka integration tests using Testcontainers.
 * <p>
 * Docker socket auto-detection tries several well-known locations when
 * {@code DOCKER_HOST} is not already set:
 * <ol>
 *   <li>{@code /var/run/docker.sock} (Linux default, macOS Docker Desktop symlink)</li>
 *   <li>{@code $HOME/.docker/run/docker.sock} (macOS Docker Desktop)</li>
 *   <li>{@code $HOME/Library/Containers/com.docker.docker/Data/docker.raw.sock}</li>
 *   <li>{@code /run/docker.sock}</li>
 * </ol>
 * <p>
 * To override, set {@code DOCKER_HOST} in your environment before running Maven,
 * or create {@code ~/.testcontainers.properties}. See {@code doc/testcontainers-setup.md}.
 */
public abstract class KafkaIntegrationTestBase {
    protected static KafkaContainer kafka;
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
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
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

    private static void configureDockerHost() {
        String env = System.getenv("DOCKER_HOST");
        if (env != null && !env.isBlank()) {
            return;
        }
        String prop = System.getProperty("DOCKER_HOST");
        if (prop != null && !prop.isBlank()) {
            return;
        }

        String socket = findDockerSocket();
        if (socket != null) {
            System.setProperty("DOCKER_HOST", "unix://" + socket);
        }
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

        for (String candidate : candidates) {
            if (canConnect(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static void addIfSocket(List<String> candidates, String path) {
        if (Files.exists(Path.of(path))) {
            candidates.add(path);
        }
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
