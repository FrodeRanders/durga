package org.gautelis.durga.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.uuid.Generators;

import java.util.Arrays;
import java.security.SecureRandom;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Injects UUIDs into specified payload fields.
 *
 * <p>Config syntax:
 * <pre>
 * "fields=id,trace_id;strategy=uuid4"
 * </pre>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code fields} — comma-separated field paths (dot-notation for nested)</li>
 *   <li>{@code strategy} — {@code uuid7} (default), {@code uuid1} (time-based v1),
 *       {@code random} (alias for uuid7)</li>
 * </ul>
 */
public final class UuidInject implements Plugin {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    @Override
    public String execute(String payload, String config) throws Exception {
        String fieldsList = "id";
        String strategy = "uuid7";
        if (config != null && !config.isBlank()) {
            String[] parts = config.split(";");
            for (String part : parts) {
                part = part.trim();
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String val = part.substring(eq + 1).trim();
                    switch (key) {
                        case "fields" -> fieldsList = val;
                        case "strategy" -> strategy = val;
                    }
                }
            }
        }
        return inject(payload, fieldsList, strategy);
    }

    public UuidInject() {
    }

    public static String inject(String json, String fieldsList, String strategy) {
        Set<String> fields = Arrays.stream(fieldsList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        if (fields.isEmpty()) {
            return json;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode input;
        try {
            input = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid input JSON", e);
        }

        ObjectNode output;
        if (input.isObject()) {
            output = input.deepCopy();
        } else {
            output = mapper.createObjectNode();
            output.set("_value", input);
        }

        for (String field : fields) {
            String uuid = generate(strategy);
            PipelinePlugin.setFieldAt(output, field, mapper.getNodeFactory().textNode(uuid));
        }
        return output.toString();
    }

    private static String generate(String strategy) {
        return switch (strategy.toLowerCase()) {
            case "uuid1" -> {
                long time = System.currentTimeMillis() * 10000L + 0x01b21dd213814000L;
                long clockSeq = SECURE_RANDOM.nextLong(0x4000) | 0x8000;
                long node = SECURE_RANDOM.nextLong(0x1_0000_0000_0000L);
                yield String.format("%08x-%04x-%04x-%04x-%012x",
                        (int) (time >> 32),
                        (int) (time >> 16) & 0xFFFF,
                        (int) (time & 0xFFFF),
                        (int) clockSeq,
                        node);
            }
            case "uuid4" -> Generators.randomBasedGenerator().generate().toString();
            default -> Generators.timeBasedEpochGenerator().generate().toString(); // UUIDv7
        };
    }
}
