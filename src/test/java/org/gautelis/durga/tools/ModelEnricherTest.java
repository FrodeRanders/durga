package org.gautelis.durga.tools;

import org.junit.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class ModelEnricherTest {

    private static final boolean COMPILER_AVAILABLE = ToolProvider.getSystemJavaCompiler() != null;

    @Test
    public void shouldEnrichCustomActivityWithImplementationMetadata() throws Exception {
        assumeCompilerAvailable();

        Path workDir = Files.createTempDirectory("test-enrich-");
        try {
            Path classesDir = workDir.resolve("classes");
            Files.createDirectories(classesDir);

            Path srcDir = workDir.resolve("src");
            Path contractFile = srcDir.resolve("CustomStepContract.java");
            Files.createDirectories(contractFile.getParent());
            Files.writeString(contractFile, """
                    package org.example;
                    public interface CustomStepContract extends org.gautelis.durga.plugins.Plugin {
                    }
                    """, StandardCharsets.UTF_8);

            Path implFile = srcDir.resolve("CustomStepImpl.java");
            Files.writeString(implFile, """
                    package org.example;
                    public class CustomStepImpl implements CustomStepContract {
                        public byte[] execute(byte[] payload, String config) { return payload; }
                    }
                    """, StandardCharsets.UTF_8);

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            int result = compiler.run(null, null, null,
                    "-d", classesDir.toString(),
                    "-cp", System.getProperty("java.class.path"),
                    contractFile.toString(), implFile.toString());
            assertEquals("compilation failed", 0, result);

            Path bpmnFile = workDir.resolve("pipeline.bpmn");
            Files.writeString(bpmnFile, """
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
                    """, StandardCharsets.UTF_8);

            ModelEnricher.enrich(classesDir, bpmnFile, srcDir);

            String enriched = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertTrue("customImpl not written", enriched.contains("customImpl"));
            assertTrue("customSource not written", enriched.contains("customSource"));
            assertTrue("customHash not written", enriched.contains("customHash"));
            assertTrue("customImpl value wrong",
                    enriched.contains("org.example.CustomStepImpl"));
            assertTrue("customSource should reference .java not .class",
                    enriched.contains("CustomStepImpl.java"));

        } finally {
            deleteRecursively(workDir);
        }
    }

    @Test
    public void shouldNotModifyBpmnWhenNoImplementationFound() throws Exception {
        assumeCompilerAvailable();

        Path workDir = Files.createTempDirectory("test-noimpl-");
        try {
            Path classesDir = workDir.resolve("classes");
            Files.createDirectories(classesDir);

            Path bpmnFile = workDir.resolve("pipeline.bpmn");
            Files.writeString(bpmnFile, """
                    <?xml version="1.0" encoding="UTF-8"?>
                    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                      xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                      targetNamespace="http://bpmn.io/schema/bpmn">
                      <bpmn:process id="test_process" isExecutable="true">
                        <bpmn:serviceTask id="orphan" name="Orphan Step">
                          <bpmn:extensionElements>
                            <camunda:properties>
                              <camunda:property name="plugin" value="custom" />
                              <camunda:property name="pluginConfig" value="interface=org.example.OrphanContract" />
                            </camunda:properties>
                          </bpmn:extensionElements>
                        </bpmn:serviceTask>
                      </bpmn:process>
                    </bpmn:definitions>
                    """, StandardCharsets.UTF_8);
            String before = Files.readString(bpmnFile, StandardCharsets.UTF_8);

            ModelEnricher.enrich(classesDir, bpmnFile);

            String after = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertEquals("BPMN modified when no implementation found", before, after);

        } finally {
            deleteRecursively(workDir);
        }
    }

    @Test
    public void shouldUpdateExistingPropertiesOnReenrichment() throws Exception {
        assumeCompilerAvailable();

        Path workDir = Files.createTempDirectory("test-reenrich-");
        try {
            Path classesDir = workDir.resolve("classes");
            Files.createDirectories(classesDir);

            Path srcDir = workDir.resolve("src");
            Path contractFile = srcDir.resolve("StepContract.java");
            Files.createDirectories(contractFile.getParent());
            Files.writeString(contractFile, """
                    package org.example;
                    public interface StepContract extends org.gautelis.durga.plugins.Plugin {
                    }
                    """, StandardCharsets.UTF_8);

            Path implFile = srcDir.resolve("StepImpl.java");
            Files.writeString(implFile, """
                    package org.example;
                    public class StepImpl implements StepContract {
                        public byte[] execute(byte[] payload, String config) { return payload; }
                    }
                    """, StandardCharsets.UTF_8);

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            int result = compiler.run(null, null, null,
                    "-d", classesDir.toString(),
                    "-cp", System.getProperty("java.class.path"),
                    contractFile.toString(), implFile.toString());
            assertEquals("compilation failed", 0, result);

            Path bpmnFile = workDir.resolve("pipeline.bpmn");
            Files.writeString(bpmnFile, """
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
                    """, StandardCharsets.UTF_8);

            ModelEnricher.enrich(classesDir, bpmnFile);

            String enriched = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertTrue("new customImpl not written",
                    enriched.contains("org.example.StepImpl"));
            assertFalse("old customImpl still present",
                    enriched.contains("old.Implementation"));
            assertTrue("new customSource not written",
                    enriched.contains("StepImpl.class"));
            assertFalse("old customSource still present",
                    enriched.contains("OldImpl.class"));
            assertFalse("old hash still present",
                    enriched.contains("abc123"));

        } finally {
            deleteRecursively(workDir);
        }
    }

    private static void assumeCompilerAvailable() {
        if (!COMPILER_AVAILABLE) {
            System.out.println("Skipping test: no system Java compiler available (JRE only?)");
        }
        assertTrue("Java compiler not available", COMPILER_AVAILABLE);
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
