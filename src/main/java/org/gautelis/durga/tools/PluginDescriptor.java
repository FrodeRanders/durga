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

    static final class PluginInputSchema {
        public Map<String, Object> schema;
    }

    static final class PluginOutputSchema {
        public Map<String, Object> schema;
        public String type;
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
}
