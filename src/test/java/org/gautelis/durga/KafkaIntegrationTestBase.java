package org.gautelis.durga;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.Socket;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Kafka integration tests using Testcontainers.
 * <p>
 * Docker socket auto-detection tries several well-known locations before giving up:
 * <ol>
 *   <li>{@code DOCKER_HOST} environment variable</li>
 *   <li>{@code TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE} environment variable</li>
 *   <li>{@code /var/run/docker.sock} (Linux default, macOS Docker Desktop symlink)</li>
 *   <li>{@code $HOME/.docker/run/docker.sock} (macOS Docker Desktop)</li>
 * </ol>
 * <p>
 * To override, set {@code DOCKER_HOST=unix:///path/to/docker.sock} in your shell
 * or create {@code ~/.testcontainers.properties} with:
 * <pre>
 * docker.host=unix:///path/to/docker.sock
 * </pre>
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
            System.err.println("Set DOCKER_HOST or create ~/.testcontainers.properties with docker.host.");
            System.err.println("See doc/testcontainers-setup.md for details.");
            org.junit.Assume.assumeTrue("Docker not available", false);
            return;
        }
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
        System.setProperty("kafka.bootstrap.servers", bootstrapServers);
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

    // ---- Docker socket detection ----

    private static void configureDockerHost() {
        if (System.getenv("DOCKER_HOST") != null || System.getProperty("DOCKER_HOST") != null) {
            return;
        }

        String override = System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE");
        if (override != null && !override.isBlank()) {
            System.setProperty("DOCKER_HOST", "unix://" + override);
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
            var addr = UnixDomainSocketAddress.of(Paths.get(socketPath));
            try (var channel = SocketChannel.open(addr)) {
                return channel.isConnected();
            }
        } catch (IOException e) {
            return false;
        }
    }
}
