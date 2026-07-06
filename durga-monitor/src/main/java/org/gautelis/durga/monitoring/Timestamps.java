package org.gautelis.durga.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Defensive ISO-8601 timestamp parsing for the monitoring topology.
 *
 * <p>Lifecycle events are consumed from Kafka and may be produced by any client,
 * so timestamp strings must be treated as untrusted input. Parsing failures must
 * never propagate out of a Kafka Streams processor or aggregate, since an uncaught
 * exception there kills the stream thread.
 */
final class Timestamps {

    private static final Logger LOG = LoggerFactory.getLogger(Timestamps.class);

    private Timestamps() {
    }

    /**
     * Parses an ISO-8601 instant, returning {@code null} when the input is
     * null, blank, or malformed. Malformed input is logged at debug level.
     */
    static Instant parseOrNull(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException e) {
            LOG.debug("Ignoring malformed timestamp '{}': {}", timestamp, e.getMessage());
            return null;
        }
    }

    /**
     * Parses an ISO-8601 instant, falling back to {@code fallback} when the input
     * is null, blank, or malformed.
     */
    static Instant parseOrDefault(String timestamp, Instant fallback) {
        Instant parsed = parseOrNull(timestamp);
        return parsed != null ? parsed : fallback;
    }
}
