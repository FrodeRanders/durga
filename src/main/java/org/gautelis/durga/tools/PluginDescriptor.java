package org.gautelis.durga.tools;

import java.util.List;
import java.util.Map;

/**
 * Parsed plugin descriptor matching the YAML format.
 * <p>
 * Field names follow the YAML keys in the plugin descriptor files.
 */
final class PluginDescriptor {
    public String id;
    public String name;
    public String version;
    public String category;
    public String status;
    public String description;

    public PluginInputSchema input;
    public PluginOutputSchema output;

    public Map<String, PluginConfigField> config;

    public PluginImplementation implementation;

    public PluginMetadata metadata;

    /** Status values that are allowed for generation. */
    private static final List<String> ALLOWED_STATUS = List.of("stable", "experimental");

    /** Status values that produce a warning. */
    private static final List<String> WARNING_STATUS = List.of("experimental", "deprecated");

    boolean isAllowed() {
        return status == null || ALLOWED_STATUS.contains(status);
    }

    boolean isWarning() {
        return status != null && WARNING_STATUS.contains(status);
    }

    /**
     * Validates a plugin configuration string against the declared config schema.
     *
     * @return a list of validation errors, or empty if valid
     */
    @SuppressWarnings("unchecked")
    List<String> validateConfig(String configString) {
        if (config == null || config.isEmpty()) {
            return List.of();
        }
        Map<String, String> options = parseConfig(configString);
        List<String> errors = new java.util.ArrayList<>();

        for (Map.Entry<String, PluginConfigField> entry : config.entrySet()) {
            String fieldName = entry.getKey();
            PluginConfigField field = entry.getValue();
            String value = options.get(fieldName);

            if (value == null && field.required) {
                errors.add("Missing required config field: " + fieldName);
                continue;
            }
            if (value == null) {
                continue;
            }

            if (field.type != null) {
                switch (field.type) {
                    case "int", "long" -> {
                        try { Long.parseLong(value); } catch (NumberFormatException e) {
                            errors.add(fieldName + ": expected integer, got '" + value + "'");
                        }
                    }
                    case "float", "double" -> {
                        try { Double.parseDouble(value); } catch (NumberFormatException e) {
                            errors.add(fieldName + ": expected number, got '" + value + "'");
                        }
                    }
                    case "boolean" -> {
                        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                            errors.add(fieldName + ": expected boolean, got '" + value + "'");
                        }
                    }
                }
            }

            if (field.values != null && !field.values.isEmpty() && !field.values.contains(value)) {
                errors.add(fieldName + ": expected one of " + field.values + ", got '" + value + "'");
            }
        }
        return errors;
    }

    private static Map<String, String> parseConfig(String config) {
        Map<String, String> options = new java.util.LinkedHashMap<>();
        if (config == null || config.isBlank()) {
            return options;
        }
        for (String part : config.split(";")) {
            part = part.trim();
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            if (eq > 0) {
                options.put(part.substring(0, eq).trim(), part.substring(eq + 1).trim());
            } else {
                options.put(part, "true");
            }
        }
        return options;
    }

    static final class PluginInputSchema {
        public Map<String, Object> schema;
        public String mediaType;
    }

    static final class PluginOutputSchema {
        public Map<String, Object> schema;
        public String mediaType;
    }

    static final class PluginConfigField {
        public String type;
        public boolean required;
        public String description;
        public String defaultValue;
        public List<String> values;
        public PluginConfigField items;
        public Map<String, PluginConfigField> properties;
    }

    static final class PluginImplementation {
        public String className;
    }

    static final class PluginMetadata {
        public List<String> inputMediaTypes;
        public List<String> outputMediaTypes;
        public String sideEffects;
        public String idempotencyStrategy;
    }
}
