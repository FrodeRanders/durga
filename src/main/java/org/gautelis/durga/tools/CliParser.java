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
                processIdOverride = args[++i];
            } else if ("--package".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --package");
                    return null;
                }
                packageName = args[++i];
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
                eventsTopic = args[++i];
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
}
