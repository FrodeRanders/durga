package org.gautelis.durga.plugins;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class ObjectStoreSupport {

    private static final String DEFAULT_ROOT = System.getProperty("java.io.tmpdir") + "/durga-object-store";
    private static final ObjectMapper LAYOUT_MAPPER = new ObjectMapper();

    private ObjectStoreSupport() {
    }

    static Map<String, String> parseConfig(String config) {
        Map<String, String> values = new LinkedHashMap<>();
        if (config == null || config.isBlank()) {
            return values;
        }
        for (String part : config.split(";")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = part.substring(0, eq).trim();
            String value = part.substring(eq + 1).trim();
            if (!key.isEmpty()) {
                values.put(key, value);
            }
        }
        return values;
    }

    static Path rootPath(Map<String, String> config) {
        String root = firstNonBlank(config.get("store"), config.get("root"), config.get("rootUri"));
        if (root == null) {
            root = DEFAULT_ROOT;
        }
        if (root.startsWith("file:")) {
            return Path.of(URI.create(root));
        }
        if (root.contains("://")) {
            throw new IllegalArgumentException("Only local file object stores are supported by this plugin");
        }
        return Path.of(root);
    }

    static String handleField(Map<String, String> config) {
        return firstNonBlank(config.get("handleField"), config.get("field"), "dataHandle");
    }

    static String mediaType(byte[] data, String fileName) {
        String byName = fileName != null ? probeByName(fileName) : null;
        if (byName != null) {
            return byName;
        }
        return FormatDetector.detect(data).mediaType();
    }

    static StoredObject store(byte[] payload, Map<String, String> config) throws IOException {
        Path root = rootPath(config);
        String prefix = sanitizePrefix(config.getOrDefault("prefix", "objects"));
        String extension = extension(config.get("extension"), payload, config.get("fileName"));
        String id = UUID.randomUUID().toString();
        Path dir = root.resolve(prefix).normalize();
        for (String segment : layoutSegments(config.get("layout"), payload, Instant.now())) {
            dir = dir.resolve(segment).normalize();
        }
        Files.createDirectories(dir);
        Path target = dir.resolve(id + extension).normalize();
        if (!target.startsWith(root.normalize())) {
            throw new IllegalArgumentException("Object target escapes configured root");
        }
        Files.write(target, payload);
        return new StoredObject(id, target.toUri().toString(), payload.length, sha256(payload), Instant.now().toString());
    }

    /**
     * Computes the metadata a {@link #store} call would produce <em>without</em> writing anything.
     * Used in validation mode to describe the object that would have been stored while suppressing
     * the external side effect. The returned URI carries the {@code validation:} scheme to make it
     * explicit that no object was written.
     */
    static StoredObject describe(byte[] payload) {
        String id = UUID.randomUUID().toString();
        return new StoredObject(id, "validation:not-stored/" + id, payload.length, sha256(payload),
                Instant.now().toString());
    }

    /**
     * Resolves the {@code layout} config into directory segments placed between
     * the prefix and the (always UUID) filename. The layout is a {@code /}-separated
     * list of tokens, each one of:
     * <ul>
     *   <li>{@code date} / {@code date:hour} / {@code date:minute} — expands to
     *       {@code yyyy/MM/dd}[/HH][/mm] in UTC;</li>
     *   <li>{@code field:<path>} — sanitized value of a payload field (dot-notation),
     *       {@code _unknown} when absent (content / business-concept naming);</li>
     *   <li>{@code const:<text>} or a bare literal — a fixed, sanitized segment.</li>
     * </ul>
     * An empty or absent layout yields a flat structure ({@code prefix/<uuid>}).
     * Tokens may be freely combined for a mixed scheme.
     */
    static List<String> layoutSegments(String layout, byte[] payload, Instant now) {
        List<String> segments = new ArrayList<>();
        if (layout == null || layout.isBlank()) {
            return segments;
        }
        JsonNode json = null;
        boolean jsonParsed = false;
        OffsetDateTime dt = now.atOffset(ZoneOffset.UTC);
        for (String rawToken : layout.split("/")) {
            String token = rawToken.trim();
            if (token.isEmpty()) {
                continue;
            }
            if (token.equals("date") || token.startsWith("date:")) {
                String granularity = token.contains(":")
                        ? token.substring(token.indexOf(':') + 1).trim().toLowerCase(java.util.Locale.ROOT)
                        : "day";
                segments.add(String.format("%04d", dt.getYear()));
                segments.add(String.format("%02d", dt.getMonthValue()));
                segments.add(String.format("%02d", dt.getDayOfMonth()));
                if (granularity.equals("hour") || granularity.equals("minute")) {
                    segments.add(String.format("%02d", dt.getHour()));
                }
                if (granularity.equals("minute")) {
                    segments.add(String.format("%02d", dt.getMinute()));
                }
            } else if (token.startsWith("field:")) {
                if (!jsonParsed) {
                    json = parseJsonPayload(payload);
                    jsonParsed = true;
                }
                String value = null;
                if (json != null) {
                    JsonNode node = PipelinePlugin.fieldAt(json, token.substring("field:".length()).trim());
                    if (node != null && !node.isNull()) {
                        value = node.asText();
                    }
                }
                segments.add(sanitizeSegment(value != null && !value.isBlank() ? value : "_unknown"));
            } else if (token.startsWith("const:")) {
                segments.add(sanitizeSegment(token.substring("const:".length()).trim()));
            } else {
                segments.add(sanitizeSegment(token));
            }
        }
        return segments;
    }

    private static JsonNode parseJsonPayload(byte[] payload) {
        try {
            return LAYOUT_MAPPER.readTree(payload);
        } catch (IOException e) {
            return null;
        }
    }

    private static String sanitizeSegment(String value) {
        String sanitized = value.replace('\\', '/')
                .replace("/", "_")
                .replaceAll("\\.\\.", "")
                .replaceAll("[^A-Za-z0-9_.-]", "_");
        return sanitized.isBlank() ? "_" : sanitized;
    }

    static byte[] read(String uri) throws IOException {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("Object handle URI is missing");
        }
        if (!uri.startsWith("file:")) {
            throw new IllegalArgumentException("Only file: object handles are supported by this plugin");
        }
        try {
            return Files.readAllBytes(Path.of(new URI(uri)));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid object handle URI", e);
        }
    }

    private static String extension(String configured, byte[] payload, String fileName) {
        if (configured != null && !configured.isBlank()) {
            return configured.startsWith(".") ? configured : "." + configured;
        }
        String mediaType = mediaType(payload, fileName);
        return switch (mediaType) {
            case "application/json" -> ".json";
            case "text/plain" -> ".txt";
            case "text/csv" -> ".csv";
            case "application/xml", "text/xml" -> ".xml";
            default -> ".bin";
        };
    }

    private static String sanitizePrefix(String prefix) {
        String sanitized = prefix.replace('\\', '/')
                .replaceAll("^/+", "")
                .replaceAll("\\.\\.", "")
                .replaceAll("[^A-Za-z0-9_./-]", "_");
        return sanitized.isBlank() ? "objects" : sanitized;
    }

    private static String probeByName(String fileName) {
        try {
            return Files.probeContentType(Path.of(fileName));
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    static String sha256(byte[] value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String firstNonBlank(String first, String second, String third) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return third != null && !third.isBlank() ? third : null;
    }

    record StoredObject(String id, String uri, long bytes, String sha256, String createdAt) {
    }
}
