package org.gautelis.durga.plugins;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

final class ObjectStoreSupport {

    private static final String DEFAULT_ROOT = System.getProperty("java.io.tmpdir") + "/durga-object-store";

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
        Files.createDirectories(dir);
        Path target = dir.resolve(id + extension).normalize();
        if (!target.startsWith(root.normalize())) {
            throw new IllegalArgumentException("Object target escapes configured root");
        }
        Files.write(target, payload);
        return new StoredObject(id, target.toUri().toString(), payload.length, sha256(payload), Instant.now().toString());
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
