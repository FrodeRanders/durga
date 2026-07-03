package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static org.junit.Assert.*;

public class ObjectStoreExtractorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldExtractPayloadFromCollectorHandle() throws Exception {
        System.out.println("TC: loads object bytes from handle emitted by collector");
        Path root = temporaryFolder.newFolder("roundtrip").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();
        ObjectStoreExtractor extractor = new ObjectStoreExtractor();

        byte[] collected = collector.execute(
                Plugin.toBytes("{\"customer\":\"Ada\"}"),
                "store=" + root + ";prefix=customers");
        byte[] extracted = extractor.execute(collected, null);

        assertEquals("{\"customer\":\"Ada\"}", Plugin.toString(extracted));
    }

    @Test
    public void shouldExtractPayloadFromConfiguredHandleField() throws Exception {
        System.out.println("TC: loads object bytes from configured handle field");
        Path root = temporaryFolder.newFolder("configured").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();
        ObjectStoreExtractor extractor = new ObjectStoreExtractor();

        byte[] collected = collector.execute(
                Plugin.toBytes("payload"),
                "store=" + root + ";handleField=blob");
        byte[] extracted = extractor.execute(collected, "handleField=blob");

        assertEquals("payload", Plugin.toString(extracted));
    }

    @Test
    public void shouldResolveUriFromDirectHandleObject() throws Exception {
        System.out.println("TC: resolves URI from direct handle object");
        Path root = temporaryFolder.newFolder("direct").toPath();
        ObjectStoreCollector collector = new ObjectStoreCollector();
        JsonNode collected = mapper.readTree(collector.execute(Plugin.toBytes("abc"), "store=" + root));
        String handleJson = collected.get("dataHandle").toString();

        assertEquals("abc", Plugin.toString(new ObjectStoreExtractor().execute(
                Plugin.toBytes(handleJson),
                null)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectMissingHandle() throws Exception {
        System.out.println("TC: rejects payloads without object-store handle");
        new ObjectStoreExtractor().execute(Plugin.toBytes("{\"x\":1}"), null);
    }
}
