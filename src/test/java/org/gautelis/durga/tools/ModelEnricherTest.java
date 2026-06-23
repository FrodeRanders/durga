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
        System.out.println("TC: Should enrich custom activity with implementation metadata");
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
                        public String execute(String payload, String config) { return payload; }
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
        System.out.println("TC: Should not modify bpmn when no implementation is found");
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
        System.out.println("TC: Should update existing properties on reenrichment");
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
                        public String execute(String payload, String config) { return payload; }
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

    @Test
    public void shouldEmbedSourceBundleAndRestoreElsewhere() throws Exception {
        System.out.println("TC: Should embed impl + support code and restore from BPMN alone");
        assumeCompilerAvailable();

        Path workDir = Files.createTempDirectory("test-embed-");
        try {
            Path classesDir = workDir.resolve("classes");
            Files.createDirectories(classesDir);
            Path srcDir = workDir.resolve("src");
            Path pkgDir = srcDir.resolve("org/example/generated");
            Files.createDirectories(pkgDir);

            Files.writeString(pkgDir.resolve("CustomTransformContract.java"), """
                    package org.example.generated;
                    public interface CustomTransformContract extends org.gautelis.durga.plugins.Plugin {
                    }
                    """, StandardCharsets.UTF_8);
            Files.writeString(pkgDir.resolve("Helper.java"), """
                    package org.example.generated;
                    final class Helper {
                        static String tag() { return "helper"; }
                    }
                    """, StandardCharsets.UTF_8);
            Files.writeString(pkgDir.resolve("ACustomTransform.java"), """
                    package org.example.generated;
                    public class ACustomTransform implements CustomTransformContract {
                        public String execute(String payload, String config) {
                            return Helper.tag();
                        }
                    }
                    """, StandardCharsets.UTF_8);

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            int result = compiler.run(null, null, null,
                    "-d", classesDir.toString(),
                    "-cp", System.getProperty("java.class.path"),
                    pkgDir.resolve("CustomTransformContract.java").toString(),
                    pkgDir.resolve("Helper.java").toString(),
                    pkgDir.resolve("ACustomTransform.java").toString());
            assertEquals("compilation failed", 0, result);

            Path bpmnFile = workDir.resolve("pipeline.bpmn");
            Files.writeString(bpmnFile, """
                    <?xml version="1.0" encoding="UTF-8"?>
                    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                      xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                                      targetNamespace="http://bpmn.io/schema/bpmn">
                      <bpmn:process id="test_process" isExecutable="true">
                        <bpmn:serviceTask id="custom_transform" name="Custom Transform">
                          <bpmn:extensionElements>
                            <camunda:properties>
                              <camunda:property name="plugin" value="custom" />
                              <camunda:property name="pluginConfig" value="interface=org.example.generated.CustomTransformContract" />
                            </camunda:properties>
                          </bpmn:extensionElements>
                        </bpmn:serviceTask>
                      </bpmn:process>
                    </bpmn:definitions>
                    """, StandardCharsets.UTF_8);

            ModelEnricher.enrich(classesDir, bpmnFile, srcDir);

            String enriched = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertTrue("durga:source not written", enriched.contains("durga:source"));
            assertTrue("CDATA not emitted", enriched.contains("<![CDATA["));
            assertTrue("impl path missing",
                    enriched.contains("org/example/generated/ACustomTransform.java"));
            assertTrue("support file (Helper) not embedded",
                    enriched.contains("org/example/generated/Helper.java"));
            assertFalse("contract should not be embedded",
                    enriched.contains("path=\"org/example/generated/CustomTransformContract.java\""));
            assertTrue("customHash missing", enriched.contains("customHash"));
            assertEquals("expected two embedded durga:source blocks",
                    2, countOccurrences(enriched, "<durga:source "));
            assertEquals("expected a per-file hash attribute on each durga:source",
                    2, countOccurrences(enriched, " hash=\""));

            ModelEnricher.enrich(classesDir, bpmnFile, srcDir);
            assertEquals("re-enrich should be a no-op when unchanged",
                    enriched, Files.readString(bpmnFile, StandardCharsets.UTF_8));

            String hashBefore = customHashOf(enriched);
            Files.writeString(pkgDir.resolve("Helper.java"), """
                    package org.example.generated;
                    final class Helper {
                        static String tag() { return "helper-CHANGED"; }
                    }
                    """, StandardCharsets.UTF_8);
            int recompiled = compiler.run(null, null, null,
                    "-d", classesDir.toString(),
                    "-cp", System.getProperty("java.class.path"),
                    pkgDir.resolve("CustomTransformContract.java").toString(),
                    pkgDir.resolve("Helper.java").toString(),
                    pkgDir.resolve("ACustomTransform.java").toString());
            assertEquals("recompilation failed", 0, recompiled);

            ModelEnricher.enrich(classesDir, bpmnFile, srcDir);
            String afterTransitive = Files.readString(bpmnFile, StandardCharsets.UTF_8);
            assertNotEquals("transitive-only change must change customHash",
                    hashBefore, customHashOf(afterTransitive));
            assertTrue("updated transitive source must be re-embedded",
                    afterTransitive.contains("helper-CHANGED"));

            Path restoreDir = workDir.resolve("restored");
            Files.createDirectories(restoreDir);
            int written = ModelEnricher.restore(bpmnFile, restoreDir);
            assertEquals("expected impl + helper restored", 2, written);

            Path restoredImpl = restoreDir.resolve("org/example/generated/ACustomTransform.java");
            Path restoredHelper = restoreDir.resolve("org/example/generated/Helper.java");
            assertTrue("impl not restored", Files.exists(restoredImpl));
            assertTrue("helper not restored", Files.exists(restoredHelper));
            assertEquals("restored impl content mismatch",
                    Files.readString(pkgDir.resolve("ACustomTransform.java"), StandardCharsets.UTF_8),
                    Files.readString(restoredImpl, StandardCharsets.UTF_8));

            int second = ModelEnricher.restore(bpmnFile, restoreDir);
            assertEquals("restore should not rewrite identical files", 0, second);

        } finally {
            deleteRecursively(workDir);
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    private static String customHashOf(String bpmn) {
        String marker = "name=\"customHash\" value=\"";
        int start = bpmn.indexOf(marker);
        assertTrue("customHash property missing", start >= 0);
        start += marker.length();
        int end = bpmn.indexOf('"', start);
        return bpmn.substring(start, end);
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
