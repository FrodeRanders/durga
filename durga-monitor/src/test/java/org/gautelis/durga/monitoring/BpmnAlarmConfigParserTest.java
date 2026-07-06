package org.gautelis.durga.monitoring;

import org.gautelis.durga.ProcessEvent;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.Assert.*;

public class BpmnAlarmConfigParserTest {

    @Test
    public void shouldParseActivityLevelAlarm() {
        System.out.println("TC: parses activity-level HARD_ERROR alarm from Camunda properties");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="my_process" isExecutable="true">
                    <bpmn:serviceTask id="validate_data">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="plugin" value="json-schema-validator" />
                          <camunda:property name="durga:alarm:validate-escalation:syndrome" value="HARD_ERROR" />
                          <camunda:property name="durga:alarm:validate-escalation:eventType" value="ACTIVITY_ESCALATED" />
                          <camunda:property name="durga:alarm:validate-escalation:severity" value="CRITICAL" />
                          <camunda:property name="durga:alarm:validate-escalation:message" value="Validation escalated in ${activityId}" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("my_process:validate-escalation", c.id());
        assertEquals("my_process", c.processId());
        assertEquals("validate_data", c.activityId());
        assertEquals(ProcessEvent.EventType.ACTIVITY_ESCALATED, c.eventType());
        assertEquals(AlarmSyndrome.HARD_ERROR, c.syndrome());
        assertEquals(AlarmSeverity.CRITICAL, c.severity());
        assertTrue(c.message().contains("validate_data"));
    }

    @Test
    public void shouldUseNormalizedActivityNameWhenPresent() {
        System.out.println("TC: alarm activity IDs use normalized BPMN names to match generated runtime events");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="order_proc" isExecutable="true">
                    <bpmn:serviceTask id="Task_123" name="Validate Order">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:validation:syndrome" value="HARD_ERROR" />
                          <camunda:property name="durga:alarm:validation:eventType" value="ACTIVITY_ESCALATED" />
                          <camunda:property name="durga:alarm:validation:severity" value="CRITICAL" />
                          <camunda:property name="durga:alarm:validation:message" value="Validation escalated in ${activityId}" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());
        assertEquals("validate_order", configs.get(0).activityId());
    }

    @Test
    public void shouldParseProcessLevelInheritedAlarm() {
        System.out.println("TC: parses process-level inherited (*) alarm, creating one per activity");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="order_proc" isExecutable="true">
                    <bpmn:extensionElements>
                      <camunda:properties>
                        <camunda:property name="durga:alarm:*default:syndrome" value="COUNTED" />
                        <camunda:property name="durga:alarm:*default:eventType" value="PROCESS_FAILED" />
                        <camunda:property name="durga:alarm:*default:threshold" value="3" />
                        <camunda:property name="durga:alarm:*default:severity" value="WARN" />
                        <camunda:property name="durga:alarm:*default:message" value="Activity ${activityId} failed ${count} times" />
                      </camunda:properties>
                    </bpmn:extensionElements>
                    <bpmn:serviceTask id="task_a" />
                    <bpmn:serviceTask id="task_b" />
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(2, configs.size());

        AlarmConfig a = configs.get(0);
        assertEquals("order_proc:default", a.id());
        assertEquals("order_proc", a.processId());
        assertEquals("task_a", a.activityId());
        assertEquals(AlarmSyndrome.COUNTED, a.syndrome());
        assertEquals(3, a.threshold());

        AlarmConfig b = configs.get(1);
        assertEquals("task_b", b.activityId());
    }

    @Test
    public void shouldParseProcessLevelAggregateAlarm() {
        System.out.println("TC: parses process-level aggregate ($) alarm with activityId=null");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="pipeline" isExecutable="true">
                    <bpmn:extensionElements>
                      <camunda:properties>
                        <camunda:property name="durga:alarm:$burst:syndrome" value="SLIDING_WINDOW" />
                        <camunda:property name="durga:alarm:$burst:eventType" value="PROCESS_FAILED" />
                        <camunda:property name="durga:alarm:$burst:threshold" value="3" />
                        <camunda:property name="durga:alarm:$burst:windowSeconds" value="60" />
                        <camunda:property name="durga:alarm:$burst:severity" value="CRITICAL" />
                        <camunda:property name="durga:alarm:$burst:message" value="${count} failures in 60s across ${processId}" />
                      </camunda:properties>
                    </bpmn:extensionElements>
                    <bpmn:serviceTask id="task_x" />
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("pipeline:burst", c.id());
        assertEquals("pipeline", c.processId());
        assertNull(c.activityId());
        assertEquals(AlarmSyndrome.SLIDING_WINDOW, c.syndrome());
        assertEquals(3, c.threshold());
        assertEquals(Duration.ofSeconds(60), c.windowDuration());
        assertEquals(AlarmSeverity.CRITICAL, c.severity());
    }

    @Test
    public void shouldCombineAllLevels() {
        System.out.println("TC: combines activity-level, inherited, and aggregate configs from one model");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="proc" isExecutable="true">
                    <bpmn:extensionElements>
                      <camunda:properties>
                        <camunda:property name="durga:alarm:*default:syndrome" value="COUNTED" />
                        <camunda:property name="durga:alarm:*default:eventType" value="PROCESS_FAILED" />
                        <camunda:property name="durga:alarm:*default:threshold" value="5" />
                        <camunda:property name="durga:alarm:*default:severity" value="WARN" />
                        <camunda:property name="durga:alarm:*default:message" value="Per-activity: ${count}" />
                        <camunda:property name="durga:alarm:$global:syndrome" value="COUNTED" />
                        <camunda:property name="durga:alarm:$global:eventType" value="PROCESS_FAILED" />
                        <camunda:property name="durga:alarm:$global:threshold" value="10" />
                        <camunda:property name="durga:alarm:$global:severity" value="CRITICAL" />
                        <camunda:property name="durga:alarm:$global:message" value="Global: ${count}" />
                      </camunda:properties>
                    </bpmn:extensionElements>
                    <bpmn:serviceTask id="task_1">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:hard-fail:syndrome" value="HARD_ERROR" />
                          <camunda:property name="durga:alarm:hard-fail:eventType" value="PROCESS_FAILED" />
                          <camunda:property name="durga:alarm:hard-fail:severity" value="CRITICAL" />
                          <camunda:property name="durga:alarm:hard-fail:message" value="Hard fail on ${activityId}" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                    <bpmn:serviceTask id="task_2" />
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        // 1 activity-level (hard-fail on task_1) + 2 inherited (default on task_1, task_2) + 1 aggregate (global)
        assertEquals(4, configs.size());

        assertEquals(1, configs.stream().filter(c -> c.activityId() != null && "HARD_ERROR".equals(c.syndrome().name())).count());
        assertEquals(2, configs.stream().filter(c -> "COUNTED".equals(c.syndrome().name()) && c.activityId() != null).count());
        assertEquals(1, configs.stream().filter(c -> c.activityId() == null).count());
    }

    @Test
    public void shouldReturnEmptyForInvalidXml() {
        System.out.println("TC: returns empty list for invalid BPMN XML");
        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse("not valid xml");
        assertTrue(configs.isEmpty());
    }

    @Test
    public void shouldSkipIncompleteAlarmConfigs() {
        System.out.println("TC: skips alarm configs missing required fields");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="p" isExecutable="true">
                    <bpmn:serviceTask id="t">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:incomplete:syndrome" value="HARD_ERROR" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertTrue(configs.isEmpty());
    }

    @Test
    public void shouldParseStuckAlarmWithoutEventType() {
        System.out.println("TC: parses activity-level STUCK alarm (no eventType, windowSeconds = idle timeout)");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="order_proc" isExecutable="true">
                    <bpmn:serviceTask id="await_payment">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:idle:syndrome" value="STUCK" />
                          <camunda:property name="durga:alarm:idle:windowSeconds" value="45" />
                          <camunda:property name="durga:alarm:idle:severity" value="WARN" />
                          <camunda:property name="durga:alarm:idle:message" value="${processInstanceId} idle in ${activityId}" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("order_proc:idle", c.id());
        assertEquals("await_payment", c.activityId());
        assertEquals(AlarmSyndrome.STUCK, c.syndrome());
        assertNull(c.eventType());
        assertEquals(Duration.ofSeconds(45), c.windowDuration());
        assertEquals(AlarmOrigin.EXPLICIT, c.origin());
    }

    @Test
    public void shouldParseCascadeAggregateAlarm() {
        System.out.println("TC: parses process-level aggregate ($) CASCADE alarm");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="pipeline" isExecutable="true">
                    <bpmn:extensionElements>
                      <camunda:properties>
                        <camunda:property name="durga:alarm:$surge:syndrome" value="CASCADE" />
                        <camunda:property name="durga:alarm:$surge:threshold" value="4" />
                        <camunda:property name="durga:alarm:$surge:windowSeconds" value="30" />
                        <camunda:property name="durga:alarm:$surge:severity" value="CRITICAL" />
                        <camunda:property name="durga:alarm:$surge:message" value="${count} instances stalled in ${processId}" />
                      </camunda:properties>
                    </bpmn:extensionElements>
                    <bpmn:serviceTask id="task_x" />
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("pipeline:surge", c.id());
        assertEquals("pipeline", c.processId());
        assertNull(c.activityId());
        assertNull(c.eventType());
        assertEquals(AlarmSyndrome.CASCADE, c.syndrome());
        assertEquals(4, c.threshold());
        assertEquals(Duration.ofSeconds(30), c.windowDuration());
        assertEquals(AlarmSeverity.CRITICAL, c.severity());
    }

    @Test
    public void shouldParseActivityLevelSlaLatency() {
        System.out.println("TC: parses an activity-level SLA_LATENCY alarm with maxLatencyMs");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="order_proc" isExecutable="true">
                    <bpmn:serviceTask id="score_risk">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:risk-sla:syndrome" value="SLA_LATENCY" />
                          <camunda:property name="durga:alarm:risk-sla:maxLatencyMs" value="2500" />
                          <camunda:property name="durga:alarm:risk-sla:severity" value="WARN" />
                          <camunda:property name="durga:alarm:risk-sla:message" value="${activityId} slow: ${latencyMs}ms > ${limitMs}ms" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("order_proc:risk-sla", c.id());
        assertEquals("order_proc", c.processId());
        assertEquals("score_risk", c.activityId());
        assertNull(c.eventType());
        assertEquals(AlarmSyndrome.SLA_LATENCY, c.syndrome());
        assertEquals(Duration.ofMillis(2500), c.windowDuration());
        assertEquals(AlarmSeverity.WARN, c.severity());
    }

    @Test
    public void shouldParseProcessLevelSlaThroughput() {
        System.out.println("TC: parses a process-level ($) SLA_THROUGHPUT alarm with minimum rate and window");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="pipeline" isExecutable="true">
                    <bpmn:extensionElements>
                      <camunda:properties>
                        <camunda:property name="durga:alarm:$min-rate:syndrome" value="SLA_THROUGHPUT" />
                        <camunda:property name="durga:alarm:$min-rate:threshold" value="100" />
                        <camunda:property name="durga:alarm:$min-rate:windowSeconds" value="60" />
                        <camunda:property name="durga:alarm:$min-rate:severity" value="CRITICAL" />
                        <camunda:property name="durga:alarm:$min-rate:message" value="throughput ${count} under ${threshold}/${windowSeconds}s" />
                      </camunda:properties>
                    </bpmn:extensionElements>
                    <bpmn:serviceTask id="task_x" />
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("pipeline:min-rate", c.id());
        assertEquals("pipeline", c.processId());
        assertNull(c.activityId());
        assertEquals(AlarmSyndrome.SLA_THROUGHPUT, c.syndrome());
        assertEquals(100, c.threshold());
        assertEquals(Duration.ofSeconds(60), c.windowDuration());
        assertEquals(AlarmSeverity.CRITICAL, c.severity());
    }

    @Test
    public void shouldTransliterateSwedishProcessAndActivityIds() {
        System.out.println("TC: Swedish ids in the model are transliterated the same way generated events are");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="återköp" name="Återköpsflöde" isExecutable="true">
                    <bpmn:serviceTask id="st" name="Bedöm återköp">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:sla:syndrome" value="SLA_LATENCY" />
                          <camunda:property name="durga:alarm:sla:maxLatencyMs" value="2000" />
                          <camunda:property name="durga:alarm:sla:severity" value="WARN" />
                          <camunda:property name="durga:alarm:sla:message" value="${activityId} slow" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn);
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("aterkop:sla", c.id());
        assertEquals("aterkop", c.processId());
        assertEquals("bedom_aterkop", c.activityId());
        assertEquals(AlarmSyndrome.SLA_LATENCY, c.syndrome());
    }

    @Test
    public void shouldUseProcessIdOverrideForConfigScope() {
        System.out.println("TC: parse honors a process-id override so configs align with --process-id events");

        String bpmn = """
                <?xml version="1.0" encoding="UTF-8"?>
                <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                  targetNamespace="http://example.com">
                  <bpmn:process id="model_internal_id" isExecutable="true">
                    <bpmn:serviceTask id="task_a" name="Task A">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="durga:alarm:fail:syndrome" value="HARD_ERROR" />
                          <camunda:property name="durga:alarm:fail:eventType" value="ACTIVITY_ESCALATED" />
                          <camunda:property name="durga:alarm:fail:severity" value="CRITICAL" />
                          <camunda:property name="durga:alarm:fail:message" value="failed in ${processId}" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                    </bpmn:serviceTask>
                  </bpmn:process>
                </bpmn:definitions>
                """;

        List<AlarmConfig> configs = BpmnAlarmConfigParser.parse(bpmn, "deployed_name");
        assertEquals(1, configs.size());

        AlarmConfig c = configs.get(0);
        assertEquals("deployed_name", c.processId());
        assertEquals("deployed_name:fail", c.id());
        assertTrue(c.message().contains("deployed_name"));
    }
}
