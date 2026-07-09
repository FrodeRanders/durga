package org.gautelis.durga.demo;

/**
 * Small argument-parsing helpers for the demo/publisher {@code main} entry points.
 * These tools are developer utilities, so invalid numeric input falls back to a
 * sensible default with a warning rather than crashing with a raw stack trace.
 */
final class DemoArgs {

    private DemoArgs() {
    }

    /**
     * Parses a long, returning {@code fallback} (with a stderr warning) when the
     * value is null or not a valid number.
     */
    static long parseLong(String value, long fallback, String name) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            System.err.println("Invalid numeric value for " + name + ": '" + value
                    + "', using default " + fallback);
            return fallback;
        }
    }

    /**
     * Returns the value following a flag at {@code args[valueIndex]}, or exits with
     * a clear message when the flag was supplied without a following value.
     */
    static String requireValue(String[] args, int valueIndex, String flag) {
        if (valueIndex >= args.length) {
            System.err.println("Missing value for " + flag);
            System.exit(1);
        }
        return args[valueIndex];
    }
}
