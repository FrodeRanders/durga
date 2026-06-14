package org.gautelis.durga.monitoring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Global metrics registry for component-level observability.
 * <p>
 * Provides counters, timers, and gauges for individual software components
 * (plugins, HTTP handlers, Kafka producers/consumers) in addition to the
 * process-level monitoring topology.
 */
public final class Metrics {

    private static final MeterRegistry REGISTRY = new SimpleMeterRegistry();

    private Metrics() {
    }

    public static MeterRegistry registry() {
        return REGISTRY;
    }

    public static String scrape() {
        var sb = new StringBuilder();
        REGISTRY.forEachMeter(meter -> {
            meter.measure().forEach(measurement -> {
                sb.append(meter.getId().getName())
                        .append("{");
                meter.getId().getTags().forEach(tag ->
                        sb.append(tag.getKey()).append("=\"").append(tag.getValue()).append("\","));
                if (!meter.getId().getTags().isEmpty()) {
                    sb.setLength(sb.length() - 1);
                }
                sb.append("} ")
                        .append(measurement.getValue())
                        .append("\n");
            });
        });
        return sb.toString();
    }
}
