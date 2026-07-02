package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class ObjectStoreCollectorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldStorePayloadAndEmitDataHandle() throws Exception {
        System.out.println("TC: stores incoming payload and emits DataHandle with format metadata");
        Path root = temporaryFolder.newFolder("objects").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();

        JsonNode result = mapper.readTree(collector.execute(
                Plugin.toBytes("{\"orderId\":7}"),
                "store=" + root + ";prefix=orders;asset=RawOrders;schema=schemas/raw-orders.json"));

        JsonNode handle = result.get("dataHandle");
        assertEquals("RawOrders", handle.get("name").asText());
        assertEquals("application/json", handle.get("mediaType").asText());
        assertEquals("schemas/raw-orders.json", handle.get("schema").asText());
        assertTrue(handle.get("uri").asText().startsWith("file:"));
        assertEquals("json", result.get("format").get("format").asText());

        Path storedFile = Path.of(java.net.URI.create(handle.get("uri").asText()));
        assertTrue(Files.exists(storedFile));
        assertEquals("{\"orderId\":7}", Files.readString(storedFile));
    }

    @Test
    public void shouldSupportConfiguredHandleField() throws Exception {
        System.out.println("TC: emits object handle under configured field name");
        Path root = temporaryFolder.newFolder("custom-field").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();

        JsonNode result = mapper.readTree(collector.execute(
                Plugin.toBytes("hello"),
                "store=" + root + ";handleField=payloadRef;includeFormat=false"));

        assertNotNull(result.get("payloadRef"));
        assertNull(result.get("format"));
    }
}
