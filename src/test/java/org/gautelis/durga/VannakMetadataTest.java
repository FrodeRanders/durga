package org.gautelis.durga;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class VannakMetadataTest {

    @Test
    public void shouldCreatePluginMetadataEventFromProcessEventAndOutputPayload() {
        System.out.println("TC: creates Vannak metadata event from plugin input and output payload");
        ProcessEvent input = new ProcessEvent(
                "instance-a",
                "pipeline-a",
                "transform",
                "token-1",
                "corr-1",
                Map.of("tenantId", "tenant-a", "environmentId", "prod", "id", "customer-42"),
                ProcessEvent.Status.STARTED,
                null,
                ProcessEvent.EventType.ACTIVITY_ENTERED,
                "v1",
                "business-42",
                "2026-07-02T10:00:00Z");

        Map<String, Object> handle = Map.of(
                "name", "RawCustomer",
                "uri", "file:///tmp/customer-42.json",
                "mediaType", "application/json");
        DataIndividualMetadataEvent event = VannakMetadata.pluginEvent(
                input,
                "collect_payload",
                "object-store-collector",
                "store=/tmp/durga",
                "{\"id\":\"customer-42\"}",
                Map.of("dataHandle", handle, "format", Map.of("format", "json")));

        assertEquals("customer-42", event.dataIndividualId());
        assertEquals("tenant-a", event.tenantId());
        assertEquals("prod", event.environmentId());
        assertEquals("pipeline-a", event.pipelineId());
        assertEquals("instance-a", event.processInstanceId());
        assertEquals("collect_payload", event.activityId());
        assertEquals(DataIndividualMetadataEvent.Operation.PERSISTED, event.operation());
        assertEquals("object-store-collector", event.activeMetadata().get("durga:plugin"));
        assertEquals("file:///tmp/customer-42.json", event.sourcePayloadRef());
        assertFalse(event.dataIndividualShardId().signum() < 0);
    }

    @Test
    public void shouldClassifyCommonPluginOperations() {
        System.out.println("TC: classifies plugin metadata operation from plugin id");
        ProcessEvent input = new ProcessEvent(
                "instance-a",
                "pipeline-a",
                "mask",
                "token-1",
                "corr-1",
                Map.of("id", "row-1"),
                ProcessEvent.Status.STARTED,
                null);

        DataIndividualMetadataEvent event = VannakMetadata.pluginEvent(
                input,
                "mask",
                "mask",
                "fields=ssn",
                "{\"id\":\"row-1\"}",
                Map.of("id", "row-1"));

        assertEquals(DataIndividualMetadataEvent.Operation.MASKED, event.operation());
    }
}
