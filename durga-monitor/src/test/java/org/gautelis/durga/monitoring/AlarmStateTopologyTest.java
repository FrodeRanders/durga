package org.gautelis.durga.monitoring;

import org.apache.kafka.streams.Topology;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AlarmStateTopologyTest {

    @Test
    public void shouldBuildAlarmStateStoreTopology() {
        System.out.println("TC: alarm state topology builds a materialized read model");

        Topology topology = AlarmStateTopology.buildTopology(
                FaultDetectionTopology.DEFAULT_ALARMS_TOPIC,
                AlarmStateTopology.DEFAULT_ALARM_STATE_STORE);

        String description = topology.describe().toString();
        assertTrue(description.contains(FaultDetectionTopology.DEFAULT_ALARMS_TOPIC));
        assertTrue(description.contains(AlarmStateTopology.DEFAULT_ALARM_STATE_STORE));
    }
}
