package org.gautelis.durga.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses command-line arguments for the BPMN scaffolder.
 */
final class CliParser {

    private static final String USAGE =
            "Usage: BpmnScaffolder <path-to-bpmn.xml> [--out <dir>] [--process-id <id>] [--package <pkg>] "
                    + "[--event-topic <topic>] [--retention <h|d|w>] [--dry-run] [--transactions] "
                    + "[--separate-workers] [--strimzi] [--connect]";

    private static final String PROCESS_ID_PATTERN = "[a-zA-Z0-9][a-zA-Z0-9_-]*";
    private static final String PACKAGE_PATTERN = "[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*";
    private static final String TOPIC_PATTERN = "[a-zA-Z0-9._-]+";
    private static final int MAX_ARG_LENGTH = 255;

    private CliParser() {
    }

    static ParsedArgs parse(String[] args) {
        if (args.length == 0) {
            System.err.println(USAGE);
            return null;
        }
        boolean dryRun = false;
        boolean transactions = false;
        boolean separateWorkers = false;
        boolean connect = false;
        boolean strimzi = false;
        String outputDir = null;
        String processIdOverride = null;
        String packageName = null;
        String retentionHours = null;
        String eventsTopic = null;
        List<String> positional = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--dry-run".equals(arg)) {
                dryRun = true;
            } else if ("--transactions".equals(arg)) {
                transactions = true;
            } else if ("--separate-workers".equals(arg)) {
                separateWorkers = true;
            } else if ("--connect".equals(arg)) {
                connect = true;
            } else if ("--strimzi".equals(arg)) {
                strimzi = true;
            } else if (arg.startsWith("--out=")) {
                outputDir = arg.substring("--out=".length());
            } else if ("--out".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --out");
                    return null;
                }
                outputDir = args[++i];
            } else if ("--process-id".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --process-id");
                    return null;
                }
                processIdOverride = validateArg(args[++i], PROCESS_ID_PATTERN, "--process-id");
                if (processIdOverride == null) return null;
            } else if ("--package".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --package");
                    return null;
                }
                packageName = validateArg(args[++i], PACKAGE_PATTERN, "--package");
                if (packageName == null) return null;
            } else if ("--retention".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --retention");
                    return null;
                }
                retentionHours = args[++i];
            } else if ("--event-topic".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --event-topic");
                    return null;
                }
                eventsTopic = validateArg(args[++i], TOPIC_PATTERN, "--event-topic");
                if (eventsTopic == null) return null;
            } else {
                positional.add(arg);
            }
        }

        if (positional.isEmpty()) {
            System.err.println(USAGE);
            return null;
        }

        String bpmnPath = positional.get(0);
        if (outputDir == null) {
            outputDir = positional.size() > 1 ? positional.get(1) : "generated";
        }
        return new ParsedArgs(bpmnPath, outputDir, dryRun, transactions, separateWorkers, connect, strimzi, processIdOverride, packageName, retentionHours, eventsTopic);
    }

    private static String validateArg(String value, String pattern, String argName) {
        if (value == null || value.isBlank()) {
            System.err.println(argName + " must not be blank");
            return null;
        }
        if (value.length() > MAX_ARG_LENGTH) {
            System.err.println(argName + " exceeds maximum length of " + MAX_ARG_LENGTH);
            return null;
        }
        if (!value.matches(pattern)) {
            System.err.println(argName + " contains invalid characters. Allowed pattern: " + pattern);
            return null;
        }
        return value;
    }
}
