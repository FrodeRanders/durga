package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Routes a JSON message to a named output channel based on the value of a
 * field in the payload.
 *
 * <p>Configuration is a map of field values to channel identifiers.
 * Messages whose field value has no route fall through to {@code defaultRoute}.
 */
public final class DeadLetterRouter implements Plugin {

    @Override
    public String execute(String payload, String config) throws Exception {
        String field = "status";
        String defaultRoute = "default";
        java.util.Map<String, String> routes = new java.util.LinkedHashMap<>();
        if (config != null && !config.isBlank()) {
            String[] parts = config.split("\\s+");
            for (String part : parts) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "field" -> field = val;
                        case "default" -> defaultRoute = val;
                        case "routes" -> {
                            if (val.startsWith("{") && val.endsWith("}")) {
                                val = val.substring(1, val.length() - 1);
                                for (String pair : val.split("\\s*,\\s*")) {
                                    int colon = pair.indexOf(':');
                                    if (colon > 0) {
                                        routes.put(pair.substring(0, colon).trim(),
                                                pair.substring(colon + 1).trim());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        DeadLetterRouter router = new DeadLetterRouter(field, routes, defaultRoute);
        return router.route(payload);
    }

    /**
     * A router's output is a routing key, not a payload; the input payload must be forwarded
     * unchanged. Declares {@link PluginResult.OutputDisposition#PASSTHROUGH} so the generated
     * worker does not overwrite the payload with the route key.
     */
    @Override
    public PluginResult executeWithResult(byte[] payload, String config) throws Exception {
        byte[] output = execute(payload, config);
        return PluginResult.passthrough(output, idempotencyKey(payload, config));
    }

    private final String field;
    private final Map<String, String> routes;
    private final String defaultRoute;

    public DeadLetterRouter() {
        this("status", Map.of(), "default");
    }

    public DeadLetterRouter(String field, Map<String, String> routes, String defaultRoute) {
        this.field = field;
        this.routes = new LinkedHashMap<>(routes);
        this.defaultRoute = defaultRoute;
    }

    /**
     * Determines the routing target for a message.
     *
     * @param json input JSON string
     * @return the route name (channel suffix or key), or {@code defaultRoute} if no match
     */
    public String route(String json) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node;
        try {
            node = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            return defaultRoute;
        }
        JsonNode fieldNode = PipelinePlugin.fieldAt(node, field);
        if (fieldNode == null || fieldNode.isNull()) {
            return defaultRoute;
        }
        String value = fieldNode.asText();
        return routes.getOrDefault(value, defaultRoute);
    }

    public String field() {
        return field;
    }

    public Map<String, String> routes() {
        return Map.copyOf(routes);
    }

    public String defaultRoute() {
        return defaultRoute;
    }
}
