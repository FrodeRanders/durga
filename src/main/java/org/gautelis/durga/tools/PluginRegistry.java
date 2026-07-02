package org.gautelis.durga.tools;

import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads and validates plugin descriptors from the {@code plugins/} directory.
 * <p>
 * The registry is loaded from {@code plugins/catalog.yml} which references
 * individual descriptor files by relative path.
 */
final class PluginRegistry {

    private final Map<String, PluginDescriptor> plugins;

    private PluginRegistry(Map<String, PluginDescriptor> plugins) {
        this.plugins = Collections.unmodifiableMap(plugins);
    }

    PluginRegistry() {
        this.plugins = Map.of();
    }

    /**
     * Returns the descriptor for a plugin id, or null if not found.
     */
    PluginDescriptor get(String pluginId) {
        return plugins.get(pluginId);
    }

    /**
     * Checks whether a plugin id is registered.
     */
    boolean contains(String pluginId) {
        return plugins.containsKey(pluginId);
    }

    /**
     * All registered plugin ids.
     */
    Iterable<String> pluginIds() {
        return plugins.keySet();
    }

    /**
     * Loads the plugin registry from a directory containing {@code catalog.yml}
     * and individual descriptor files.
     */
    @SuppressWarnings("unchecked")
    static PluginRegistry load(Path registryDir) throws IOException {
        Path catalogFile = registryDir.resolve("catalog.yml");
        if (!Files.exists(catalogFile)) {
            return new PluginRegistry(Map.of());
        }

        LoadSettings settings = LoadSettings.builder().build();
        Load loader = new Load(settings);
        Map<String, Object> catalog = (Map<String, Object>) loader.loadFromString(
                Files.readString(catalogFile));
        List<Map<String, Object>> catalogPlugins = (List<Map<String, Object>>) catalog.get("plugins");

        Map<String, PluginDescriptor> plugins = new LinkedHashMap<>();
        if (catalogPlugins != null) {
            for (Map<String, Object> entry : catalogPlugins) {
                String id = (String) entry.get("id");
                String path = (String) entry.get("path");
                if (id == null || path == null) {
                    System.err.println("Warning: skipping catalog entry with missing id or path");
                    continue;
                }
                Path descriptorFile = registryDir.resolve(path);
                if (!Files.exists(descriptorFile)) {
                    System.err.println("Warning: plugin descriptor not found: " + descriptorFile);
                    continue;
                }
                Map<String, Object> descMap = (Map<String, Object>) loader.loadFromString(
                        Files.readString(descriptorFile));
                PluginDescriptor desc = parseDescriptor(id, descMap);
                if (desc != null) {
                    plugins.put(desc.id, desc);
                }
            }
        }

        return new PluginRegistry(plugins);
    }

    /**
     * Loads the plugin registry from a classpath catalog URL.
     */
    @SuppressWarnings("unchecked")
    static PluginRegistry load(URL catalogUrl) throws IOException {
        LoadSettings settings = LoadSettings.builder().build();
        Load loader = new Load(settings);
        String catalogYaml;
        try (var input = catalogUrl.openStream()) {
            catalogYaml = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        Map<String, Object> catalog = (Map<String, Object>) loader.loadFromString(catalogYaml);
        List<Map<String, Object>> catalogPlugins = (List<Map<String, Object>>) catalog.get("plugins");

        Map<String, PluginDescriptor> plugins = new LinkedHashMap<>();
        if (catalogPlugins != null) {
            for (Map<String, Object> entry : catalogPlugins) {
                String id = (String) entry.get("id");
                String path = (String) entry.get("path");
                if (id == null || path == null) {
                    continue;
                }
                String resourcePath = "/plugins/" + path;
                URL descUrl = PluginRegistry.class.getResource(resourcePath);
                if (descUrl == null) {
                    System.err.println("Warning: plugin descriptor not found on classpath: " + resourcePath);
                    continue;
                }
                String descYaml;
                try (var input = descUrl.openStream()) {
                    descYaml = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                }
                Map<String, Object> descMap = (Map<String, Object>) loader.loadFromString(descYaml);
                PluginDescriptor desc = parseDescriptor(id, descMap);
                if (desc != null) {
                    plugins.put(desc.id, desc);
                }
            }
        }
        return new PluginRegistry(plugins);
    }

    @SuppressWarnings("unchecked")
    private static PluginDescriptor parseDescriptor(String id, Map<String, Object> descMap) {
        PluginDescriptor desc = new PluginDescriptor();
        desc.id = id;
        desc.name = (String) descMap.get("name");
        desc.version = (String) descMap.get("version");
        desc.category = (String) descMap.get("category");
        desc.status = (String) descMap.get("status");
        desc.description = (String) descMap.get("description");

        Map<String, Object> impl = (Map<String, Object>) descMap.get("implementation");
        if (impl != null) {
            desc.implementation = new PluginDescriptor.PluginImplementation();
            desc.implementation.className = (String) impl.get("class");
        }

        Map<String, Object> input = (Map<String, Object>) descMap.get("input");
        if (input != null) {
            desc.input = new PluginDescriptor.PluginInputSchema();
            desc.input.schema = (Map<String, Object>) input.get("schema");
            desc.input.mediaType = (String) input.get("mediaType");
        }

        Map<String, Object> output = (Map<String, Object>) descMap.get("output");
        if (output != null) {
            desc.output = new PluginDescriptor.PluginOutputSchema();
            desc.output.schema = (Map<String, Object>) output.get("schema");
            desc.output.mediaType = (String) output.get("mediaType");
        }

        Map<String, Object> metadata = (Map<String, Object>) descMap.get("metadata");
        if (metadata != null) {
            desc.metadata = new PluginDescriptor.PluginMetadata();
            desc.metadata.inputMediaTypes = (List<String>) metadata.get("inputMediaTypes");
            desc.metadata.outputMediaTypes = (List<String>) metadata.get("outputMediaTypes");
            desc.metadata.sideEffects = (String) metadata.get("sideEffects");
            desc.metadata.idempotencyStrategy = (String) metadata.get("idempotencyStrategy");
        }

        Map<String, Object> config = (Map<String, Object>) descMap.get("config");
        if (config != null) {
            desc.config = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : config.entrySet()) {
                PluginDescriptor.PluginConfigField field = new PluginDescriptor.PluginConfigField();
                Object rawValue = entry.getValue();
                if (rawValue instanceof Map<?, ?> fieldDef) {
                    field.type = (String) fieldDef.get("type");
                    field.required = Boolean.TRUE.equals(fieldDef.get("required"));
                    field.description = (String) fieldDef.get("description");
                    field.defaultValue = fieldDef.get("defaultValue") != null
                            ? String.valueOf(fieldDef.get("defaultValue")) : null;
                    @SuppressWarnings("unchecked")
                    List<String> values = (List<String>) fieldDef.get("values");
                    field.values = values;
                } else {
                    field.description = String.valueOf(rawValue);
                }
                desc.config.put(entry.getKey(), field);
            }
        }

        if (!desc.isAllowed()) {
            System.err.println("Warning: plugin '" + desc.id + "' has status '" + desc.status
                    + "' and will not be available for generation");
            return null;
        }
        if (desc.isWarning()) {
            System.err.println("Note: plugin '" + desc.id + "' has status '" + desc.status + "'");
        }
        if (desc.implementation == null || desc.implementation.className == null) {
            System.err.println("Warning: plugin '" + desc.id + "' has no implementation class");
            return null;
        }
        return desc;
    }
}
