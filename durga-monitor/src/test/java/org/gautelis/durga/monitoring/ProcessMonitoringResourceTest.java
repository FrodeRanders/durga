package org.gautelis.durga.monitoring;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProcessMonitoringResourceTest {

    @Test
    public void shouldEscapePrometheusLabelValues() {
        System.out.println("TC: escapes prometheus label values");
        assertEquals("proc\\\\\\\"x\\ny", ProcessMonitoringResource.prometheusLabelValue("proc\\\"x\ny"));
    }
}
