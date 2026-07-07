package org.gautelis.durga.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema governance utilities for validating and comparing JSON Schemas
 * at scaffold time and during generated-project compilation.
 */
public final class SchemaSupport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SchemaSupport() {
    }

    /**
     * Validates that a JSON Schema file is syntactically correct.
     *
     * @param schemaPath path to a JSON Schema file
     * @return list of validation errors, empty if valid
     */
    public static List<String> validateSchema(Path schemaPath) {
        List<String> errors = new ArrayList<>();
        if (!Files.exists(schemaPath)) {
            errors.add("Schema file not found: " + schemaPath);
            return errors;
        }
        try {
            String content = Files.readString(schemaPath);
            JsonNode schema = MAPPER.readTree(content);
            if (!schema.isObject()) {
                errors.add("Schema root must be an object");
                return errors;
            }
            if (!schema.has("type") && !schema.has("properties") && !schema.has("$ref")
                    && !schema.has("oneOf") && !schema.has("anyOf") && !schema.has("allOf")) {
                errors.add("Schema missing type, properties, or $ref");
            }
            if (schema.has("properties")) {
                JsonNode props = schema.get("properties");
                if (!props.isObject()) {
                    errors.add("Properties must be an object");
                }
            }
            if (schema.has("required")) {
                JsonNode required = schema.get("required");
                if (!required.isArray()) {
                    errors.add("Required must be an array");
                }
            }
        } catch (IOException e) {
            errors.add("Failed to read schema file: " + e.getMessage());
        }
        return errors;
    }

    /**
     * Checks whether two schemas are compatible (backward-compatible extension).
     * Returns a list of incompatibilities. Empty list means compatible.
     *
     * @param before path to the previous schema version
     * @param after  path to the new schema version
     * @return list of incompatibility descriptions
     */
    public static List<String> checkCompatibility(Path before, Path after) {
        List<String> issues = new ArrayList<>();
        if (!Files.exists(before)) {
            issues.add("Previous schema not found: " + before);
            return issues;
        }
        if (!Files.exists(after)) {
            issues.add("New schema not found: " + after);
            return issues;
        }
        try {
            JsonNode oldSchema = MAPPER.readTree(Files.readString(before));
            JsonNode newSchema = MAPPER.readTree(Files.readString(after));
            checkPropertyCompatibility(oldSchema, newSchema, issues, "");
        } catch (IOException e) {
            issues.add("Failed to read schema: " + e.getMessage());
        }
        return issues;
    }

    /**
     * Infers a Java class name suitable for a generated record matching this schema.
     *
     * @param schemaPath path to a JSON Schema file
     * @param baseName   suggested base name for the class
     * @return a PascalCase class name
     */
    public static String inferClassName(Path schemaPath, String baseName) {
        if (baseName != null && !baseName.isBlank()) {
            return BpmnModelCollector.toClassName(baseName);
        }
        String fileName = schemaPath.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String name = dot > 0 ? fileName.substring(0, dot) : fileName;
        return BpmnModelCollector.toClassName(name.replace("-", "_"));
    }

    /**
     * Resolves a data object schema reference (from camunda:property 'schema') to
     * a filesystem or classpath path relative to the generated project resources.
     *
     * @param schemaRef    the schema value (e.g. "schemas/normalized-orders.schema.json")
     * @param resourcesDir the src/main/resources directory of the generated project
     * @return resolved path, or null if not found
     */
    public static Path resolveSchemaRef(String schemaRef, Path resourcesDir) {
        if (schemaRef == null || schemaRef.isBlank()) {
            return null;
        }
        if (schemaRef.startsWith("classpath:")) {
            return null;
        }
        Path resolved = resourcesDir.resolve(schemaRef).normalize();
        if (Files.exists(resolved)) {
            return resolved;
        }
        if (schemaRef.contains("/") && !schemaRef.startsWith("/")) {
            resolved = resourcesDir.getParent().resolve(schemaRef).normalize();
            if (Files.exists(resolved)) {
                return resolved;
            }
        }
        if (!schemaRef.startsWith("/")) {
            resolved = resourcesDir.resolve("schemas").resolve(schemaRef).normalize();
            if (Files.exists(resolved)) {
                return resolved;
            }
        }
        return null;
    }

    /**
     * Generates a JSON Schema string for a simple record type from a set of field definitions.
     *
     * @param fields map of field name to JSON Schema type
     * @param requiredFields list of required field names
     * @return JSON Schema string
     */
    public static String generateRecordSchema(Map<String, String> fields, List<String> requiredFields) {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = MAPPER.createObjectNode();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            ObjectNode property = MAPPER.createObjectNode();
            property.put("type", entry.getValue());
            properties.set(entry.getKey(), property);
        }
        schema.set("properties", properties);
        if (requiredFields != null && !requiredFields.isEmpty()) {
            ArrayNode required = MAPPER.createArrayNode();
            requiredFields.forEach(required::add);
            schema.set("required", required);
        }
        schema.put("additionalProperties", false);
        return schema.toPrettyString();
    }

    private static void checkPropertyCompatibility(JsonNode oldSchema, JsonNode newSchema,
                                                    List<String> issues, String path) {
        if (!oldSchema.isObject() || !newSchema.isObject()) {
            return;
        }

        JsonNode oldProps = oldSchema.get("properties");
        JsonNode newProps = newSchema.get("properties");
        if (oldProps == null || !oldProps.isObject()) {
            return;
        }
        if (newProps == null || !newProps.isObject()) {
            return;
        }

        for (Map.Entry<String, JsonNode> entry : oldProps.properties()) {
            String field = entry.getKey();
            JsonNode oldProp = entry.getValue();
            JsonNode newProp = newProps.get(field);

            String fieldPath = path.isEmpty() ? field : path + "." + field;
            if (newProp == null) {
                issues.add("Field removed: " + fieldPath);
                continue;
            }

            String oldType = oldProp.has("type") ? oldProp.get("type").asText() : null;
            String newType = newProp.has("type") ? newProp.get("type").asText() : null;
            if (oldType != null && newType != null && !oldType.equals(newType)) {
                issues.add("Type changed for " + fieldPath + ": " + oldType + " -> " + newType);
            }

            if ("object".equals(oldType) && "object".equals(newType)) {
                checkPropertyCompatibility(oldProp, newProp, issues, fieldPath);
            }
        }

        JsonNode oldRequired = oldSchema.get("required");
        JsonNode newRequired = newSchema.get("required");
        if (oldRequired != null && oldRequired.isArray() && newRequired != null && newRequired.isArray()) {
            List<String> oldReq = new ArrayList<>();
            List<String> newReq = new ArrayList<>();
            oldRequired.forEach(n -> oldReq.add(n.asText()));
            newRequired.forEach(n -> newReq.add(n.asText()));
            for (String req : newReq) {
                if (!oldReq.contains(req)) {
                    String fieldPath = path.isEmpty() ? req : path + "." + req;
                    issues.add("Required field added: " + fieldPath);
                }
            }
        }
    }
}
