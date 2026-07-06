package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.Topology;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class ValidationTopologyTest {

    @Test
    public void shouldBuildValidationComparisonTopology() {
        System.out.println("TC: validation topology wires candidate + prior sources into a materialized results store");

        ValidationTopology.ValidationTopics topics = new ValidationTopology.ValidationTopics(
                ValidationTopology.DEFAULT_CANDIDATE_TOPIC,
                ProcessMonitoringTopology.DEFAULT_EVENTS_TOPIC,
                null,
                ValidationTopology.DEFAULT_RESULTS_TOPIC,
                ValidationTopology.DEFAULT_RESULTS_STORE,
                List.of("meta.timestamp"));

        Topology topology = ValidationTopology.buildTopology(topics);
        String description = topology.describe().toString();

        assertTrue(description.contains(ValidationTopology.DEFAULT_CANDIDATE_TOPIC));
        assertTrue(description.contains(ProcessMonitoringTopology.DEFAULT_EVENTS_TOPIC));
        assertTrue(description.contains(ValidationTopology.DEFAULT_RESULTS_TOPIC));
        assertTrue(description.contains(ValidationTopology.DEFAULT_RESULTS_STORE));
    }

    @Test
    public void forAllProcessesUsesConfiguredIgnorePaths() {
        System.out.println("TC: ValidationTopics.forAllProcesses reads ignore paths from the system property");
        String previous = System.getProperty("durga.validation.ignore.paths");
        System.setProperty("durga.validation.ignore.paths", "meta.ts, order.processedAt ,");
        try {
            ValidationTopology.ValidationTopics topics = ValidationTopology.ValidationTopics.forAllProcesses();
            assertTrue(topics.ignorePaths().contains("meta.ts"));
            assertTrue(topics.ignorePaths().contains("order.processedAt"));
            assertTrue(topics.ignorePaths().size() == 2);
        } finally {
            if (previous == null) {
                System.clearProperty("durga.validation.ignore.paths");
            } else {
                System.setProperty("durga.validation.ignore.paths", previous);
            }
        }
    }
}
