package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.ServiceTask;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperties;
import org.camunda.bpm.model.bpmn.instance.camunda.CamundaProperty;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;

/**
 * Enriches a local BPMN model with implementation metadata discovered from
 * compiled Java classes that implement generated contract interfaces.
 *
 * <p>Uses a {@link URLClassLoader} to load compiled classes and check
 * interface implementations via reflection. Hashes source files rather
 * than bytecode so the hash is stable across JDK versions and compiler flags.
 *
 * <p>Usage:
 * <pre>
 *   java -cp target/classes:... org.gautelis.durga.tools.ModelEnricher \
 *       target/classes src/main/resources/pipeline.bpmn [src/main/java]
 * </pre>
 *
 * <p>If the source directory is omitted, it is derived from the classes
 * directory by replacing {@code target/classes} with {@code src/main/java}
 * (Maven convention).
 */
public final class ModelEnricher {

    private ModelEnricher() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ModelEnricher <classes-dir> <bpmn-file> [sources-dir]");
            System.exit(1);
        }
        Path classesDir = Path.of(args[0]);
        Path bpmnFile = Path.of(args[1]);
        Path sourcesDir = args.length >= 3 ? Path.of(args[2]) : null;

        if (!Files.exists(classesDir) || !Files.isDirectory(classesDir)) {
            System.err.println("Classes directory not found: " + classesDir);
            System.exit(1);
        }
        if (!Files.exists(bpmnFile)) {
            System.err.println("BPMN file not found: " + bpmnFile);
            System.exit(1);
        }

        enrich(classesDir, bpmnFile, sourcesDir);
    }

    /**
     * Scans compiled classes and enriches the BPMN model with implementation metadata.
     * Derives the source directory from the classes directory by Maven convention.
     */
    public static void enrich(Path classesDir, Path bpmnFile) throws IOException {
        enrich(classesDir, bpmnFile, null);
    }

    /**
     * Scans compiled classes and enriches the BPMN model with implementation metadata.
     *
     * @param classesDir compiled classes root (e.g. {@code target/classes})
     * @param bpmnFile   the BPMN model to enrich in-place
     * @param sourcesDir source files root (e.g. {@code src/main/java}); if null,
     *                   derived from {@code classesDir} by Maven convention
     */
    public static void enrich(Path classesDir, Path bpmnFile, Path sourcesDir) throws IOException {
        if (sourcesDir == null) {
            sourcesDir = deriveSourcesDir(classesDir);
        }

        String bpmnContent = Files.readString(bpmnFile, StandardCharsets.UTF_8);
        BpmnModelInstance model = Bpmn.readModelFromStream(
                new java.io.ByteArrayInputStream(bpmnContent.getBytes(StandardCharsets.UTF_8)));

        URLClassLoader classLoader = null;
        boolean modified = false;
        Collection<ServiceTask> serviceTasks = model.getModelElementsByType(ServiceTask.class);

        for (ServiceTask task : serviceTasks) {
            String pluginId = getProperty(task, "plugin");
            if (!"custom".equals(pluginId)) {
                continue;
            }

            String pluginConfig = getProperty(task, "pluginConfig");
            String contractFqName = parseContractFqName(pluginConfig, task);
            if (contractFqName == null) {
                continue;
            }

            if (classLoader == null) {
                classLoader = new URLClassLoader(new URL[]{classesDir.toUri().toURL()},
                        ClassLoader.getSystemClassLoader());
            }

            Path implClassPath = findImplementation(classesDir, contractFqName, classLoader);
            if (implClassPath == null) {
                continue;
            }

            String implClassName = classNameFromPath(classesDir, implClassPath);
            Path implSourcePath = classToSource(sourcesDir, implClassPath, classesDir);
            String hash = implSourcePath != null && Files.exists(implSourcePath)
                    ? sha256(implSourcePath) : sha256(implClassPath);
            String sourceName = implSourcePath != null
                    ? sourcesDir.relativize(implSourcePath).toString()
                    : implClassPath.getFileName().toString();
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

        if (classLoader != null) {
            try {
                classLoader.close();
            } catch (IOException ignored) {
            }
        }

        if (modified) {
            String updatedBpmn = Bpmn.convertToString(model);
            Files.writeString(bpmnFile, updatedBpmn, StandardCharsets.UTF_8);
            System.out.println("BPMN model enriched: " + bpmnFile);
        } else {
            System.out.println("No changes needed for: " + bpmnFile);
        }
    }

    /**
     * Derives the source directory from the classes directory using the
     * Maven convention: {@code target/classes → src/main/java}.
     */
    static Path deriveSourcesDir(Path classesDir) {
        Path normalized = classesDir.toAbsolutePath().normalize();
        String path = normalized.toString();
        String targetClasses = path.endsWith("/") ? "target/classes/" : "target/classes";
        int idx = path.lastIndexOf(targetClasses);
        if (idx >= 0) {
            return Path.of(path.substring(0, idx) + "src/main/java");
        }
        return null;
    }

    /**
     * Converts a compiled class path to the corresponding source file path.
     * For example, {@code target/classes/org/example/Foo.class} →
     * {@code src/main/java/org/example/Foo.java}.
     */
    private static Path classToSource(Path sourcesDir, Path classFile, Path classesDir) {
        if (sourcesDir == null) {
            return null;
        }
        Path relative = classesDir.relativize(classFile);
        String relStr = relative.toString();
        if (relStr.endsWith(".class")) {
            relStr = relStr.substring(0, relStr.length() - 6) + ".java";
        }
        return sourcesDir.resolve(relStr);
    }

    private static String parseContractFqName(String pluginConfig, ServiceTask task) {
        if (pluginConfig == null || pluginConfig.isBlank()) {
            String name = task.getName() != null && !task.getName().isBlank()
                    ? task.getName() : task.getId();
            return BpmnScaffolder.toClassName(name) + "Contract";
        }
        for (String part : pluginConfig.split(";")) {
            part = part.trim();
            if (part.startsWith("interface=")) {
                return part.substring("interface=".length()).trim();
            }
        }
        String trimmed = pluginConfig.trim();
        if (trimmed.contains(".")) {
            return trimmed;
        }
        return null;
    }

    private static Path findImplementation(Path classesDir, String contractFqName,
                                            URLClassLoader classLoader) {
        Class<?> contractClass;
        try {
            contractClass = classLoader.loadClass(contractFqName);
        } catch (ClassNotFoundException e) {
            return null;
        }

        String contractPackage = contractFqName.contains(".")
                ? contractFqName.substring(0, contractFqName.lastIndexOf('.'))
                : "";
        String packagePath = contractPackage.replace('.', '/');
        Path packageDir = classesDir.resolve(packagePath);

        if (!Files.isDirectory(packageDir)) {
            return null;
        }

        try (var stream = Files.list(packageDir)) {
            List<Path> candidates = stream
                    .filter(p -> p.getFileName().toString().endsWith(".class"))
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return !name.contains("$") && !name.equals(contractFqName.substring(
                                contractFqName.lastIndexOf('.') + 1) + ".class");
                    })
                    .toList();

            for (Path candidate : candidates) {
                String candidateName = classNameFromPath(classesDir, candidate);
                try {
                    Class<?> candidateClass = classLoader.loadClass(candidateName);
                    if (contractClass.isAssignableFrom(candidateClass)
                            && candidateClass != contractClass
                            && !candidateClass.isInterface()) {
                        return candidate;
                    }
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    continue;
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
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

    private static String classNameFromPath(Path classesDir, Path classFile) {
        Path relative = classesDir.relativize(classFile);
        String path = relative.toString().replace('/', '.');
        if (path.endsWith(".class")) {
            path = path.substring(0, path.length() - 6);
        }
        return path;
    }

    static String sha256(Path file) {
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
