package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Secure BPMN model parsing with XXE hardening and path validation.
 */
final class SafeXml {

    private static final Set<String> ALLOWED_EXTENSIONS = Set.of(".xml", ".bpmn", ".bpmn2");

    private SafeXml() {
    }

    /**
     * Reads a BPMN model from a file with XXE protections.
     * Content is read, validated, then parsed via a stream to ensure hardening applies.
     */
    static BpmnModelInstance readModelFromFile(File file) {
        if (file == null) {
            throw new IllegalArgumentException("BPMN file must not be null");
        }
        validateFileExtension(file);
        try {
            String xml = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            preValidateXml(xml);
            return Bpmn.readModelFromStream(
                    new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse BPMN model: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Reads a BPMN model from an XML string with XXE protections.
     */
    static BpmnModelInstance readModelFromString(String xml) {
        if (xml == null || xml.isBlank()) {
            throw new IllegalArgumentException("BPMN content must not be blank");
        }
        preValidateXml(xml);
        return Bpmn.readModelFromStream(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Validates and normalizes a CLI-supplied path, rejecting traversal attempts.
     *
     * @param path the user-supplied path
     * @return canonical absolute path
     */
    static Path safePath(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Path must not be blank");
        }
        Path resolved = Path.of(path).normalize().toAbsolutePath();
        String str = resolved.toString();
        if (str.contains("..")) {
            throw new IllegalArgumentException("Path traversal rejected: " + path);
        }
        return resolved;
    }

    /**
     * Validates that a filename has an allowed extension.
     */
    private static void validateFileExtension(File file) {
        String name = file.getName().toLowerCase();
        boolean allowed = ALLOWED_EXTENSIONS.stream().anyMatch(name::endsWith);
        if (!allowed && name.contains(".")) {
            throw new IllegalArgumentException(
                    "BPMN file must have .xml, .bpmn, or .bpmn2 extension: " + file.getName());
        }
    }

    /**
     * Pre-validates XML content using a hardened parser that rejects
     * external entities, doctype declarations, and XIncludes.
     */
    private static void preValidateXml(String xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            factory.setNamespaceAware(true);

            var builder = factory.newDocumentBuilder();
            builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("BPMN XML failed security validation: " + e.getMessage(), e);
        }
    }
}
