package org.gautelis.durga.monitoring;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gautelis.durga.ProcessEvent;

import java.util.Objects;

/**
 * A triggered alarm emitted when fault detection threshold is crossed.
 *
 * @param alarmId            unique alarm instance identifier
 * @param configId           reference to the triggering {@link AlarmConfig#id()}
 * @param syndrome           which detection pattern triggered
 * @param severity           alarm severity
 * @param message            rendered alarm message
 * @param processId          process where the fault was observed
 * @param processInstanceId  specific process instance
 * @param activityId         specific activity (may be null for process-wide alarms)
 * @param triggerEventType   the lifecycle event type that pushed the count over threshold
 * @param triggerTimestamp   ISO-8601 instant when the alarm fired
 * @param count              current accumulated count (1 for HARD_ERROR)
 * @param threshold          configured threshold (ignored for HARD_ERROR)
 */
public record AlarmEvent(
        String alarmId,
        String configId,
        AlarmSyndrome syndrome,
        AlarmSeverity severity,
        String message,
        String processId,
        String processInstanceId,
        String activityId,
        ProcessEvent.EventType triggerEventType,
        String triggerTimestamp,
        int count,
        int threshold
) {
    public AlarmEvent {
        Objects.requireNonNull(alarmId, "alarmId");
        Objects.requireNonNull(configId, "configId");
        Objects.requireNonNull(syndrome, "syndrome");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(processId, "processId");
        Objects.requireNonNull(triggerEventType, "triggerEventType");
        Objects.requireNonNull(triggerTimestamp, "triggerTimestamp");
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize AlarmEvent", e);
        }
    }

    public static AlarmEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, AlarmEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse AlarmEvent", e);
        }
    }
}
