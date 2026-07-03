package org.gautelis.durga;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.*;

public class ProcessStateStoreIntegrationTest extends KafkaIntegrationTestBase {
    private static final String TOPIC = "process-state-it-" + UUID.randomUUID().toString().substring(0, 8);
    private ProcessStateStore store;

    @BeforeClass
    public static void createCompactTopic() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(TOPIC, 1, (short) 1);
            topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
            admin.createTopics(List.of(topic)).all().get();
        }
    }

    @Before
    public void setUp() {
        store = new ProcessStateStore(bootstrapServers(), TOPIC, "test-proc");
    }

    @After
    public void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void shouldSaveAndLoadLatest() {
        System.out.println("TC: saves a ProcessState and loads it back via loadLatest on a compacted topic");
        ProcessState state = new ProcessState(
                "pi-1",
                List.of(new ProcessState.Token("token-1", "task-A")),
                Map.of("v1", "hello"),
                1L
        );
        store.save(state);

        ProcessState loaded = store.loadLatest("pi-1", Duration.ofSeconds(10));
        assertNotNull(loaded);
        assertEquals("pi-1", loaded.processInstanceId());
        assertEquals(1L, loaded.version());
        assertEquals("hello", loaded.variables().get("v1"));
        assertEquals(1, loaded.tokens().size());
        assertEquals("token-1", loaded.tokens().get(0).tokenId());
    }

    @Test
    public void shouldReturnLatestAfterMultipleSaves() {
        System.out.println("TC: multiple saves to the same key return the latest version via loadLatest");
        store.save(new ProcessState("pi-2", List.of(), Map.of("ver", 1), 1L));
        store.save(new ProcessState("pi-2", List.of(), Map.of("ver", 2), 2L));
        store.save(new ProcessState("pi-2", List.of(), Map.of("ver", 3), 3L));

        ProcessState loaded = store.loadLatest("pi-2", Duration.ofSeconds(10));
        assertNotNull(loaded);
        assertEquals(3L, loaded.version());
        assertEquals(3, loaded.variables().get("ver"));
    }

    @Test
    public void shouldHandleMultipleKeys() {
        System.out.println("TC: saves distinct keys and loads each one independently");
        store.save(new ProcessState("pi-a", List.of(), Map.of("key", "a"), 1L));
        store.save(new ProcessState("pi-b", List.of(), Map.of("key", "b"), 1L));
        store.save(new ProcessState("pi-c", List.of(), Map.of("key", "c"), 1L));

        ProcessState loadedA = store.loadLatest("pi-a", Duration.ofSeconds(10));
        ProcessState loadedB = store.loadLatest("pi-b", Duration.ofSeconds(10));
        ProcessState loadedC = store.loadLatest("pi-c", Duration.ofSeconds(10));

        assertEquals("a", loadedA.variables().get("key"));
        assertEquals("b", loadedB.variables().get("key"));
        assertEquals("c", loadedC.variables().get("key"));
    }

    @Test
    public void shouldReturnNullWhenKeyNotFound() {
        System.out.println("TC: loadLatest returns null when no state exists for the given key");
        ProcessState loaded = store.loadLatest("pi-nonexistent", Duration.ofSeconds(5));
        assertNull(loaded);
    }

    @Test
    public void shouldHandleTimeoutGracefully() {
        System.out.println("TC: loadLatest returns null gracefully even with a very short timeout");
        ProcessState loaded = store.loadLatest("pi-timeout", Duration.ofMillis(0));
        assertNull(loaded);
    }
}
