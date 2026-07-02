package org.gautelis.durga;

import org.junit.Test;

import java.math.BigInteger;
import java.util.Map;

import static org.junit.Assert.*;

public class DataIndividualMetadataEventTest {

    @Test
    public void shouldRoundTripVannakMetadataEventJson() {
        System.out.println("TC: round-trips Vannak data-individual metadata event JSON");
        DataIndividualMetadataEvent event = new DataIndividualMetadataEvent(
                "meta-1",
                "data-1",
                DataIndividualMetadataEvent.shardIdFor("data-1"),
                "tenant-a",
                "prod",
                "pipeline-a",
                "instance-a",
                "transform",
                "2026-07-02T10:00:00Z",
                DataIndividualMetadataEvent.Operation.TRANSFORMED,
                Map.of("format", "json"),
                Map.of("plugin", "json-transform"),
                "file:///tmp/data-1.json",
                "data-1:meta-1");

        DataIndividualMetadataEvent parsed = DataIndividualMetadataEvent.fromJson(event.toJson());

        assertEquals(event, parsed);
        assertEquals(new BigInteger("17378634421404960907"),
                DataIndividualMetadataEvent.shardIdFor("data-1"));
    }

    @Test
    public void shouldDefaultIdsAndMetadataMaps() {
        System.out.println("TC: defaults metadata event id, idempotency key and metadata maps");
        DataIndividualMetadataEvent event = new DataIndividualMetadataEvent(
                null,
                null,
                BigInteger.ZERO,
                "default",
                "default",
                "pipeline-a",
                "instance-a",
                "activity-a",
                "2026-07-02T10:00:00Z",
                DataIndividualMetadataEvent.Operation.RECEIVED,
                null,
                null,
                null,
                null);

        assertEquals("instance-a", event.dataIndividualId());
        assertNotNull(event.metadataEventId());
        assertEquals("instance-a:" + event.metadataEventId(), event.idempotencyKey());
        assertTrue(event.passiveMetadata().isEmpty());
        assertTrue(event.activeMetadata().isEmpty());
    }
}
