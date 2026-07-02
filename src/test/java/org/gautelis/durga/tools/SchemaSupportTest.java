package org.gautelis.durga.tools;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaSupportTest {

    @Test
    public void shouldFlagAddedRequiredFieldAsIncompatible() throws Exception {
        System.out.println("TC: schema compatibility rejects newly required fields");
        Path dir = Files.createTempDirectory("durga-schema-compat-");
        Path before = dir.resolve("before.schema.json");
        Path after = dir.resolve("after.schema.json");
        Files.writeString(before, """
                {"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id"]}
                """);
        Files.writeString(after, """
                {"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"]}
                """);

        List<String> issues = SchemaSupport.checkCompatibility(before, after);

        assertTrue(issues.contains("Required field added: name"));
    }

    @Test
    public void shouldAllowRemovedRequiredFieldForBackwardCompatibleExtensionCheck() throws Exception {
        System.out.println("TC: schema compatibility allows removed required fields");
        Path dir = Files.createTempDirectory("durga-schema-compat-");
        Path before = dir.resolve("before.schema.json");
        Path after = dir.resolve("after.schema.json");
        Files.writeString(before, """
                {"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"]}
                """);
        Files.writeString(after, """
                {"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id"]}
                """);

        List<String> issues = SchemaSupport.checkCompatibility(before, after);

        assertFalse(issues.contains("Required field removed: name"));
        assertTrue(issues.isEmpty());
    }
}
