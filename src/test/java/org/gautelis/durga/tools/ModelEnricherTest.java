package org.gautelis.durga.tools;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class ModelEnricherTest {

    @Test
    public void shouldEnrichCustomActivityWithImplementationMetadata() throws Exception {
        System.out.println("TC: enriches BPMN model with custom impl, source, and hash properties");

        Path bpmnFile = Files.createTempFile("test-enrich-", ".bpmn");
        Path classesDir = Files.createTempDirectory("test-classes-");
        try {
            String bpmn = """
                    <?xml version="1.0" encoding="UTF-8"?>
                    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                      xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                      targetNamespace="http://bpmn.io/schema/bpmn">
                      <bpmn:process id="test_process" isExecutable="true">
                        <bpmn:serviceTask id="custom_step" name="Custom Step">
                          <bpmn:extensionElements>
                            <camunda:properties>
                              <camunda:property name="plugin" value="custom" />
                              <camunda:property name="pluginConfig" value="interface=org.example.CustomStepContract" />
                            </camunda:properties>
                          </bpmn:extensionElements>
                        </bpmn:serviceTask>
                      </bpmn:process>
                    </bpmn:definitions>
                    """;
            Files.writeString(bpmnFile, bpmn, StandardCharsets.UTF_8);

            Path contractDir = classesDir.resolve("org/example");
            Files.createDirectories(contractDir);

            byte[] contractBytes = mockClassBytes("org/example/CustomStepContract", (String) null);
            Files.write(contractDir.resolve("CustomStepContract.class"), contractBytes);

            byte[] implBytes = mockClassBytes("org/example/CustomStepImpl", "org/example/CustomStepContract");
            Files.write(contractDir.resolve("CustomStepImpl.class"), implBytes);

            ModelEnricher.enrich(classesDir, bpmnFile);

            String enriched = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertTrue("customImpl not written", enriched.contains("customImpl"));
            assertTrue("customSource not written", enriched.contains("customSource"));
            assertTrue("customHash not written", enriched.contains("customHash"));
            assertTrue("customImpl value wrong",
                    enriched.contains("org.example.CustomStepImpl"));
            assertTrue("customSource value wrong",
                    enriched.contains("CustomStepImpl.class"));

        } finally {
            Files.deleteIfExists(bpmnFile);
            deleteRecursively(classesDir);
        }
    }

    @Test
    public void shouldNotModifyBpmnWhenNoImplementationFound() throws Exception {
        System.out.println("TC: does not modify BPMN when no contract implementation exists");

        Path bpmnFile = Files.createTempFile("test-noimpl-", ".bpmn");
        Path classesDir = Files.createTempDirectory("test-noclasses-");
        try {
            String bpmn = """
                    <?xml version="1.0" encoding="UTF-8"?>
                    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                      xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                      targetNamespace="http://bpmn.io/schema/bpmn">
                      <bpmn:process id="test_process" isExecutable="true">
                        <bpmn:serviceTask id="orphan" name="Orphan Step">
                          <bpmn:extensionElements>
                            <camunda:properties>
                              <camunda:property name="plugin" value="custom" />
                              <camunda:property name="pluginConfig" value="interface=com.example.OrphanContract" />
                            </camunda:properties>
                          </bpmn:extensionElements>
                        </bpmn:serviceTask>
                      </bpmn:process>
                    </bpmn:definitions>
                    """;
            Files.writeString(bpmnFile, bpmn, StandardCharsets.UTF_8);
            String before = Files.readString(bpmnFile, StandardCharsets.UTF_8);

            ModelEnricher.enrich(classesDir, bpmnFile);

            String after = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertEquals("BPMN modified when no implementation found", before, after);

        } finally {
            Files.deleteIfExists(bpmnFile);
            deleteRecursively(classesDir);
        }
    }

    @Test
    public void shouldUpdateExistingPropertiesOnReenrichment() throws Exception {
        System.out.println("TC: updates customImpl, customHash when implementation changes");

        Path bpmnFile = Files.createTempFile("test-reenrich-", ".bpmn");
        Path classesDir = Files.createTempDirectory("test-reclasses-");
        try {
            String bpmn = """
                    <?xml version="1.0" encoding="UTF-8"?>
                    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                      xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                      targetNamespace="http://bpmn.io/schema/bpmn">
                      <bpmn:process id="test_process" isExecutable="true">
                        <bpmn:serviceTask id="step" name="Step">
                          <bpmn:extensionElements>
                            <camunda:properties>
                              <camunda:property name="plugin" value="custom" />
                              <camunda:property name="pluginConfig" value="interface=org.example.StepContract" />
                              <camunda:property name="customImpl" value="old.Implementation" />
                              <camunda:property name="customSource" value="OldImpl.class" />
                              <camunda:property name="customHash" value="abc123" />
                            </camunda:properties>
                          </bpmn:extensionElements>
                        </bpmn:serviceTask>
                      </bpmn:process>
                    </bpmn:definitions>
                    """;
            Files.writeString(bpmnFile, bpmn, StandardCharsets.UTF_8);

            Path contractDir = classesDir.resolve("org/example");
            Files.createDirectories(contractDir);
            Files.write(contractDir.resolve("StepContract.class"),
                    mockClassBytes("org/example/StepContract", (String) null));
            Files.write(contractDir.resolve("StepImpl.class"),
                    mockClassBytes("org/example/StepImpl", "org/example/StepContract"));

            ModelEnricher.enrich(classesDir, bpmnFile);

            String enriched = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertTrue("old customImpl not replaced",
                    enriched.contains("org.example.StepImpl"));
            assertFalse("old customImpl still present",
                    enriched.contains("old.Implementation"));
            assertTrue("old customSource not replaced",
                    enriched.contains("StepImpl.class"));
            assertFalse("old customSource still present",
                    enriched.contains("OldImpl.class"));
            assertFalse("old hash still present",
                    enriched.contains("abc123"));

        } finally {
            Files.deleteIfExists(bpmnFile);
            deleteRecursively(classesDir);
        }
    }

    private byte[] mockClassBytes(String fqName, String implementsContract) {
        String internalName = fqName.replace('.', '/');
        StringBuilder sb = new StringBuilder();
        sb.append("CAFEBABE");
        sb.append("class:").append(internalName);
        if (implementsContract != null) {
            sb.append(";implements:").append(implementsContract.replace('.', '/'));
        }
        return sb.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    private static void deleteRecursively(Path path) {
        try {
            if (Files.isDirectory(path)) {
                try (var stream = Files.list(path)) {
                    stream.forEach(ModelEnricherTest::deleteRecursively);
                }
            }
            Files.deleteIfExists(path);
        } catch (Exception ignored) {
        }
    }
}
