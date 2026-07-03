package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class PluginExecutionSupportTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldPassPayloadThroughToPluginByDefault() throws Exception {
        System.out.println("TC: default plugin execution passes original payload bytes to plugin");
        PluginExecutionSupport.Result result = PluginExecutionSupport.execute(
                new UppercasePlugin(),
                Plugin.toBytes("hello"),
                null);

        assertEquals("HELLO", Plugin.toString(result.output()));
        assertFalse(result.materializedHandle());
    }

    @Test
    public void shouldTreatDataHandleAsPayloadWhenHandleModeIsManual() throws Exception {
        System.out.println("TC: manual handle mode passes DataHandle JSON itself to plugin");
        Path root = temporaryFolder.newFolder("manual").toPath();
        byte[] handlePayload = new ObjectStoreCollector().execute(
                Plugin.toBytes("hello"),
                "store=" + root);

        PluginExecutionSupport.Result result = PluginExecutionSupport.execute(
                new EchoPlugin(),
                handlePayload,
                "handleMode=manual");

        assertEquals(Plugin.toString(handlePayload), Plugin.toString(result.output()));
        assertFalse(result.materializedHandle());
    }

    @Test
    public void shouldMaterializeDataHandleFeedRawBytesAndEmitNewHandle() throws Exception {
        System.out.println("TC: materialized handle mode loads object bytes and stores plugin output as a new DataHandle");
        Path root = temporaryFolder.newFolder("materialized").toPath();
        byte[] handlePayload = new ObjectStoreCollector().execute(
                Plugin.toBytes("hello"),
                "store=" + root + ";asset=Greeting");

        PluginExecutionSupport.Result result = PluginExecutionSupport.execute(
                new UppercasePlugin(),
                handlePayload,
                "handleMode=materialize;store=" + root + ";asset=GreetingUpper");

        assertTrue(result.materializedHandle());
        assertEquals("hello", Plugin.toString(result.pluginInput()));

        JsonNode output = mapper.readTree(result.output());
        JsonNode handle = output.get("dataHandle");
        assertEquals("GreetingUpper", handle.get("name").asText());
        assertEquals("text/plain", handle.get("mediaType").asText());
        assertEquals("text", output.get("format").get("format").asText());

        Path stored = Path.of(java.net.URI.create(handle.get("uri").asText()));
        assertEquals("HELLO", Files.readString(stored));
    }

    @Test
    public void shouldUsePluginConfigOverrideWhenMaterializing() throws Exception {
        System.out.println("TC: materialized handle mode can pass pluginConfig override to plugin");
        Path root = temporaryFolder.newFolder("override").toPath();
        byte[] handlePayload = new ObjectStoreCollector().execute(
                Plugin.toBytes("hello"),
                "store=" + root);

        PluginExecutionSupport.Result result = PluginExecutionSupport.execute(
                new PrefixPlugin(),
                handlePayload,
                "handleMode=materialize;store=" + root + ";pluginConfig=prefix=raw:");

        JsonNode output = mapper.readTree(result.output());
        Path stored = Path.of(java.net.URI.create(output.get("dataHandle").get("uri").asText()));
        assertEquals("raw:hello", Files.readString(stored));
    }

    static final class UppercasePlugin implements Plugin {
        @Override
        public byte[] execute(byte[] payload, String config) {
            return new String(payload, StandardCharsets.UTF_8)
                    .toUpperCase(java.util.Locale.ROOT)
                    .getBytes(StandardCharsets.UTF_8);
        }
    }

    static final class EchoPlugin implements Plugin {
        @Override
        public byte[] execute(byte[] payload, String config) {
            return payload;
        }
    }

    static final class PrefixPlugin implements Plugin {
        @Override
        public byte[] execute(byte[] payload, String config) {
            String prefix = config != null && config.startsWith("prefix=")
                    ? config.substring("prefix=".length()) : "";
            return (prefix + new String(payload, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }
    }
}
