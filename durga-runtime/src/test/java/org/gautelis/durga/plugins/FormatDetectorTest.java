package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class FormatDetectorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldDetectJsonObjectPayload() throws Exception {
        System.out.println("TC: detects JSON object payload and emits MIME metadata");
        FormatDetector detector = new FormatDetector();
        JsonNode result = mapper.readTree(detector.execute(
                Plugin.toBytes("{\"id\":42,\"name\":\"Ada\"}"),
                null));

        JsonNode format = result.get("format");
        assertEquals("json", format.get("format").asText());
        assertEquals("object", format.get("datatype").asText());
        assertEquals("application/json", format.get("mediaType").asText());
        assertTrue(format.get("bytes").asLong() > 0);
        assertFalse(format.get("sha256").asText().isBlank());
    }

    @Test
    public void shouldDetectCsvPayload() {
        System.out.println("TC: detects simple CSV text payload");
        FormatDetector.Detection detection = FormatDetector.detect(
                Plugin.toBytes("id,name\n1,Ada\n2,Grace\n"));

        assertEquals("csv", detection.format());
        assertEquals("table", detection.datatype());
        assertEquals("text/csv", detection.mediaType());
    }

    @Test
    public void shouldDetectBinaryPayload() {
        System.out.println("TC: detects non-text payload as binary bytes");
        FormatDetector.Detection detection = FormatDetector.detect(new byte[]{0, 1, 2, 3});

        assertEquals("binary", detection.format());
        assertEquals("bytes", detection.datatype());
        assertEquals("application/octet-stream", detection.mediaType());
    }

    @Test
    public void shouldUseConfiguredOutputField() throws Exception {
        System.out.println("TC: writes detection metadata under configured output field");
        FormatDetector detector = new FormatDetector();
        JsonNode result = mapper.readTree(detector.execute(
                Plugin.toBytes("hello"),
                "field=payloadFormat"));

        assertNotNull(result.get("payloadFormat"));
        assertNull(result.get("format"));
    }

    @Test
    public void shouldDeclarePassthroughDisposition() throws Exception {
        System.out.println("TC: inspection result is PASSTHROUGH so the original payload is preserved downstream");
        Plugin plugin = new FormatDetector();
        PluginResult result = plugin.executeWithResult(Plugin.toBytes("{\"order_id\":7}"), ".", PluginExecutionContext.production());
        assertEquals(PluginResult.OutputDisposition.PASSTHROUGH, result.disposition());
    }
}
