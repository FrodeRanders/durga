package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.ServiceTask;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperties;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperty;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

/**
 * Enriches a local BPMN model with implementation metadata discovered from
 * compiled Java classes that implement generated contract interfaces.
 *
 * <p>Intended to be run during the build (e.g. via Maven exec plugin or a
 * dedicated Maven plugin). It scans {@code target/classes/} for classes that
 * implement a contract interface in the generated package, matches them to
 * BPMN activities, and writes {@code customImpl}, {@code customSource},
 * and {@code customHash} Camunda extension properties back into the BPMN file.
 *
 * <p>Usage:
 * <pre>
 *   java -cp target/classes:... org.gautelis.durga.tools.ModelEnricher \
 *       target/classes src/main/resources/pipeline.bpmn
 * </pre>
 */
public final class ModelEnricher {

    private ModelEnricher() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ModelEnricher <classes-dir> <bpmn-file>");
            System.exit(1);
        }
        Path classesDir = Path.of(args[0]);
        Path bpmnFile = Path.of(args[1]);

        if (!Files.exists(classesDir) || !Files.isDirectory(classesDir)) {
            System.err.println("Classes directory not found: " + classesDir);
            System.exit(1);
        }
        if (!Files.exists(bpmnFile)) {
            System.err.println("BPMN file not found: " + bpmnFile);
            System.exit(1);
        }

        enrich(classesDir, bpmnFile);
    }

    /**
     * Scans compiled classes and enriches the BPMN model with implementation metadata.
     */
    public static void enrich(Path classesDir, Path bpmnFile) throws IOException {
        String bpmnContent = Files.readString(bpmnFile, StandardCharsets.UTF_8);
        BpmnModelInstance model = Bpmn.readModelFromStream(
                new java.io.ByteArrayInputStream(bpmnContent.getBytes(StandardCharsets.UTF_8)));

        boolean modified = false;
        Collection<ServiceTask> serviceTasks = model.getModelElementsByType(ServiceTask.class);

        for (ServiceTask task : serviceTasks) {
            String pluginId = getProperty(task, "plugin");
            if (!"custom".equals(pluginId)) {
                continue;
            }

            String contractName = getProperty(task, "pluginConfig");
            if (contractName == null || contractName.isBlank()) {
                continue;
            }

            String contractSimpleName = contractName.contains(".")
                    ? contractName.substring(contractName.lastIndexOf('.') + 1)
                    : contractName;

            List<Path> impls = findImplementations(classesDir, contractSimpleName);
            if (impls.isEmpty()) {
                continue;
            }

            Path implPath = impls.getFirst();
            String implClassName = classNameFromPath(classesDir, implPath);
            String hash = sha256(implPath);
            String sourceName = implPath.getFileName().toString();
            String knownHash = getProperty(task, "customHash");

            if (hash.equals(knownHash) && implClassName.equals(getProperty(task, "customImpl"))) {
                continue;
            }

            setOrUpdateProperty(task, "customImpl", implClassName);
            setOrUpdateProperty(task, "customSource", sourceName);
            setOrUpdateProperty(task, "customHash", hash);

            System.out.println("Enriched custom activity '" + task.getId()
                    + "': impl=" + implClassName + ", source=" + sourceName
                    + (knownHash != null && !hash.equals(knownHash) ? " (hash changed)" : ""));
            modified = true;
        }

        if (modified) {
            String updatedBpmn = Bpmn.convertToString(model);
            Files.writeString(bpmnFile, updatedBpmn, StandardCharsets.UTF_8);
            System.out.println("BPMN model enriched: " + bpmnFile);
        } else {
            System.out.println("No changes needed for: " + bpmnFile);
        }
    }

    private static String getProperty(ServiceTask task, String name) {
        if (task.getExtensionElements() == null) {
            return null;
        }
        CamundaProperties props = task.getExtensionElements()
                .getElementsQuery()
                .filterByType(CamundaProperties.class)
                .singleResult();
        if (props == null || props.getCamundaProperties() == null) {
            return null;
        }
        for (CamundaProperty prop : props.getCamundaProperties()) {
            if (name.equals(prop.getCamundaName())) {
                return prop.getCamundaValue();
            }
        }
        return null;
    }

    private static void setOrUpdateProperty(ServiceTask task, String name, String value) {
        if (task.getExtensionElements() == null) {
            return;
        }
        CamundaProperties props = task.getExtensionElements()
                .getElementsQuery()
                .filterByType(CamundaProperties.class)
                .singleResult();
        if (props == null) {
            return;
        }
        for (CamundaProperty prop : props.getCamundaProperties()) {
            if (name.equals(prop.getCamundaName())) {
                prop.setCamundaValue(value);
                return;
            }
        }
        CamundaProperty newProp = task.getModelInstance()
                .newInstance(CamundaProperty.class);
        newProp.setCamundaName(name);
        newProp.setCamundaValue(value);
        props.getCamundaProperties().add(newProp);
    }

    private static List<Path> findImplementations(Path classesDir, String contractName) throws IOException {
        try (Stream<Path> stream = Files.walk(classesDir)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(".class"))
                    .filter(p -> {
                        String className = p.getFileName().toString().replace(".class", "");
                        return !className.equals(contractName) && !className.contains("$");
                    })
                    .filter(p -> implementsContract(classesDir, p, contractName))
                    .toList();
        }
    }

    private static boolean implementsContract(Path classesDir, Path classFile, String contractName) {
        try {
            byte[] bytes = Files.readAllBytes(classFile);
            String interfaceRef = ("L" + contractName.replace('.', '/') + ";").replace('/', '/');
            String constantRef = contractName.replace('.', '/');
            String content = new String(bytes, StandardCharsets.ISO_8859_1);

            return content.contains(interfaceRef) || content.contains(constantRef);
        } catch (IOException e) {
            return false;
        }
    }

    private static String classNameFromPath(Path classesDir, Path classFile) {
        Path relative = classesDir.relativize(classFile);
        String path = relative.toString().replace('/', '.');
        if (path.endsWith(".class")) {
            path = path.substring(0, path.length() - 6);
        }
        return path;
    }

    private static String sha256(Path file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream in = Files.newInputStream(file)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) {
                    digest.update(buf, 0, n);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            return "error:" + e.getMessage();
        }
    }
}
