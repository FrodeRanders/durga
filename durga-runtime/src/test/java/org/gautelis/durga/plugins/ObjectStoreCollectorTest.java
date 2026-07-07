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
    public void shouldSuppressStoreWriteInValidationMode() throws Exception {
        System.out.println("TC: validation-mode execution documents the handle without writing to the store");
        Path root = temporaryFolder.newFolder("validation-objects").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();

        PluginResult result = collector.executeWithResult(
                Plugin.toBytes("{\"orderId\":7}"),
                "store=" + root + ";prefix=orders;asset=RawOrders",
                PluginExecutionContext.validation());

        JsonNode output = mapper.readTree(result.output());
        JsonNode handle = output.get("dataHandle");
        assertEquals("RawOrders", handle.get("name").asText());
        assertEquals(PluginResult.OutputDisposition.SIDE_EFFECT, result.disposition());
        assertTrue("validation URI must mark the object as not stored",
                handle.get("uri").asText().startsWith("validation:not-stored/"));

        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            assertFalse("no object file must be written in validation mode",
                    walk.anyMatch(Files::isRegularFile));
        }
    }

    @Test
    public void shouldStoreNormallyForProductionContext() throws Exception {
        System.out.println("TC: production-context execution still writes to the store");
        Path root = temporaryFolder.newFolder("production-objects").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();

        PluginResult result = collector.executeWithResult(
                Plugin.toBytes("{\"orderId\":7}"),
                "store=" + root + ";prefix=orders",
                PluginExecutionContext.production());

        JsonNode handle = mapper.readTree(result.output()).get("dataHandle");
        assertTrue(handle.get("uri").asText().startsWith("file:"));
        Path storedFile = Path.of(java.net.URI.create(handle.get("uri").asText()));
        assertTrue(Files.exists(storedFile));
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

    @Test
    public void shouldLayStoredObjectOutByConfiguredScheme() throws Exception {
        System.out.println("TC: layout config places the stored object under date/field/const directories");
        Path root = temporaryFolder.newFolder("layout-objects").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();

        JsonNode result = mapper.readTree(collector.execute(
                Plugin.toBytes("{\"kind\":\"invoice\"}"),
                "store=" + root + ";prefix=data;layout=const:tenantA/field:kind"));

        String uri = result.get("dataHandle").get("uri").asText();
        assertTrue("expected tenantA/invoice directories, got " + uri,
                uri.contains("/data/tenantA/invoice/"));
        assertTrue(uri.endsWith(".json"));
    }

    @Test
    public void shouldExpandDateLayoutToUtcGranularity() {
        System.out.println("TC: date layout expands to y/m/d[/H/m] segments");
        java.time.Instant now = java.time.Instant.now();
        assertEquals(3, ObjectStoreSupport.layoutSegments("date", new byte[0], now).size());
        assertEquals(4, ObjectStoreSupport.layoutSegments("date:hour", new byte[0], now).size());
        assertEquals(5, ObjectStoreSupport.layoutSegments("date:minute", new byte[0], now).size());
        assertTrue(ObjectStoreSupport.layoutSegments("", new byte[0], now).isEmpty());
    }

    @Test
    public void shouldFallBackToUnknownForMissingLayoutField() {
        System.out.println("TC: field layout falls back to _unknown when the field is absent");
        java.time.Instant now = java.time.Instant.now();
        assertEquals(java.util.List.of("_unknown"),
                ObjectStoreSupport.layoutSegments("field:nope", "{}".getBytes(), now));
        assertEquals(java.util.List.of("orders_eu"),
                ObjectStoreSupport.layoutSegments("field:t", "{\"t\":\"orders/eu\"}".getBytes(), now));
    }
}
