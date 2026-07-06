package org.gautelis.durga.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BpmnScaffolderTest {
    private static void announce(String message) {
        System.out.println("TC: " + message);
    }

    @Test
    public void dryRunIncludesMessageEventArtifacts() throws Exception {
        System.out.println("TC: dry-run generates message event catch/throw services and channel names");
        String output = runDryRun("src/test/resources/bpmn/invoice_message_exchange.bpmn");

        assertTrue(output.contains("Process: invoice_message_exchange"));
        assertTrue(output.contains("Message events: [await_review_response, send_review_request]"));
        assertTrue(output.contains("AwaitReviewResponseMessageCatchService"));
        assertTrue(output.contains("SendReviewRequestMessageThrowService"));
        assertTrue(output.contains("invoice_message_exchange_invoice_review_response_message"));
    }

    @Test
    public void dryRunIncludesNestedSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates nested sub-process entry/completion services");
        String output = runDryRun("src/test/resources/bpmn/invoice_nested_subprocess.bpmn");

        assertTrue(output.contains("Embedded subprocesses: [review_scope, approval_scope]"));
        assertTrue(output.contains("ReviewScopeSubProcessEntryService"));
        assertTrue(output.contains("ReviewScopeSubProcessCompletionService"));
        assertTrue(output.contains("ApprovalScopeSubProcessEntryService"));
        assertTrue(output.contains("ApprovalScopeSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesSubProcessBoundaryErrorArtifacts() throws Exception {
        System.out.println("TC: dry-run generates boundary error event services on sub-processes");
        String output = runDryRun("src/test/resources/bpmn/invoice_subprocess_error.bpmn");

        assertTrue(output.contains("Boundary events: [review_scope_error]"));
        assertTrue(output.contains("ReviewScopeErrorBoundaryErrorService"));
        assertTrue(output.contains("ReviewScopeSubProcessEntryService"));
        assertTrue(output.contains("ReviewScopeSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesTimerArtifacts() throws Exception {
        System.out.println("TC: dry-run generates timer service and input channel");
        String output = runDryRun("src/test/resources/bpmn/invoice_receipt_reminder.bpmn");

        assertTrue(output.contains("Timers: [wait_before_notify]"));
        assertTrue(output.contains("WaitBeforeNotifyTimerService"));
        assertTrue(output.contains("invoice_receipt_reminder_wait_before_notify_input"));
    }

    @Test
    public void transactionWorkersUseConfiguredEventsTopic() throws Exception {
        System.out.println("TC: transactional workers use configured process event topic");
        Path outputDir = Files.createTempDirectory("durga-transactions-topic-");

        BpmnScaffolder.main(new String[]{
                fixturePath("src/test/resources/bpmn/invoice_receipt.bpmn").toAbsolutePath().toString(),
                "--out",
                outputDir.toAbsolutePath().toString(),
                "--transactions",
                "--event-topic",
                "custom-process-events"
        });

        String generated = Files.walk(outputDir.resolve("src/main/java"))
                .filter(path -> path.getFileName().toString().endsWith(".java"))
                .map(path -> {
                    try {
                        return Files.readString(path);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .filter(content -> content.contains("processEventsTopic"))
                .findFirst()
                .orElseThrow();
        assertTrue(generated.contains("String processEventsTopic = \"custom-process-events\";"));
        assertFalse(generated.contains("String processEventsTopic = \"process-events\";"));
    }

    @Test
    public void dryRunIncludesBoundaryEscalationArtifacts() throws Exception {
        System.out.println("TC: dry-run generates boundary escalation service and publisher");
        String output = runDryRun("src/test/resources/bpmn/invoice_review_escalation.bpmn");

        assertTrue(output.contains("Boundary events: [review_escalation]"));
        assertTrue(output.contains("ReviewEscalationBoundaryEscalationService"));
        assertTrue(output.contains("InvoiceReviewEscalationTaskEscalationPublisher"));
    }

    @Test
    public void dryRunIncludesNonInterruptingBoundaryTimerArtifacts() throws Exception {
        System.out.println("TC: dry-run generates non-interrupting boundary timer and worker service");
        String output = runDryRun("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn");

        assertTrue(output.contains("Boundary events: [review_reminder]"));
        assertTrue(output.contains("ReviewReminderBoundaryTimerService"));
        assertTrue(output.contains("SendReviewReminderWorkerService"));
    }

    @Test
    public void dryRunIncludesNonInterruptingSubProcessBoundaryTimerArtifacts() throws Exception {
        System.out.println("TC: dry-run generates non-interrupting sub-process boundary timer and worker service");
        String output = runDryRun("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn");

        assertTrue(output.contains("Boundary events: [review_scope_reminder]"));
        assertTrue(output.contains("Embedded subprocesses: [review_scope]"));
        assertTrue(output.contains("ReviewScopeReminderBoundaryTimerService"));
        assertTrue(output.contains("NotifyManagerWorkerService"));
    }

    @Test
    public void dryRunIncludesSignalArtifacts() throws Exception {
        System.out.println("TC: dry-run generates signal catch/throw services and channel names");
        String output = runDryRun("src/test/resources/bpmn/invoice_signal_exchange.bpmn");

        assertTrue(output.contains("Signal events: [await_review_signal, send_review_signal]"));
        assertTrue(output.contains("AwaitReviewSignalSignalCatchService"));
        assertTrue(output.contains("SendReviewSignalSignalThrowService"));
        assertTrue(output.contains("invoice_signal_exchange_invoice_review_signal_signal"));
    }

    @Test
    public void dryRunIncludesCallActivityArtifacts() throws Exception {
        System.out.println("TC: dry-run generates call activity service, completion publisher and shell script");
        String output = runDryRun("src/test/resources/bpmn/invoice_call_activity.bpmn");

        assertTrue(output.contains("Call activities: [validate_invoice_process]"));
        assertTrue(output.contains("ValidateInvoiceProcessCallActivityService"));
        assertTrue(output.contains("InvoiceCallActivityCallActivityCompletionPublisher"));
        assertTrue(output.contains("complete-call-activity.sh"));
    }

    @Test
    public void dryRunIncludesEventSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates event sub-process start/completion services and message channel");
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn");

        assertTrue(output.contains("Event subprocesses: [manager_notification_scope]"));
        assertTrue(output.contains("ManagerNotificationScopeEventSubProcessStartService"));
        assertTrue(output.contains("ManagerNotificationScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("invoice_event_subprocess_message_manager_note_message"));
    }

    @Test
    public void dryRunIncludesInterruptingEventSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates interrupting event sub-process start/completion services and cancellation message");
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn");

        assertTrue(output.contains("Event subprocesses: [invoice_cancellation_scope]"));
        assertTrue(output.contains("InvoiceCancellationScopeEventSubProcessStartService"));
        assertTrue(output.contains("InvoiceCancellationScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("invoice_event_subprocess_interrupting_message_cancel_invoice_message"));
    }

    @Test
    public void dryRunIncludesTimerEventSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates timer event sub-process start/completion services for nested sub-process");
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_reminder_scope]"));
        assertTrue(output.contains("ReviewReminderScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewReminderScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("Embedded subprocesses: [review_scope]"));
    }

    @Test
    public void dryRunIncludesErrorEventSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates error event sub-process start/completion services");
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_error_scope]"));
        assertTrue(output.contains("ReviewErrorScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewErrorScopeEventSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesEscalationEventSubProcessArtifacts() throws Exception {
        System.out.println("TC: dry-run generates escalation event sub-process start/completion services");
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_escalation_scope]"));
        assertTrue(output.contains("ReviewEscalationScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewEscalationScopeEventSubProcessCompletionService"));
    }

    @Test
    public void generationWritesMessageEventProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes message event catch/throw Java files, publisher and application.yml channels");
        Path outputDir = Files.createTempDirectory("durga-message-project-");

        runGeneration("src/test/resources/bpmn/invoice_message_exchange.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/AwaitReviewResponseMessageCatchService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/SendReviewRequestMessageThrowService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/probes/InvoiceMessageExchangeMessageEventPublisher.java")));
        assertTrue(Files.exists(outputDir.resolve("send-message-event.sh")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_message_exchange_invoice_review_response_message"));
        assertTrue(applicationYaml.contains("invoice_message_exchange_invoice_review_request_message"));
    }

    @Test
    public void generatedApplicationYamlStringsSequentialTaskTopicsWithoutChannelNameCollisions() throws Exception {
        System.out.println("TC: generated application.yml maps task inputs to predecessor output topics without SmallRye channel collisions");
        Path outputDir = Files.createTempDirectory("durga-pipeline-channels-");

        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);

        Map<String, Object> yaml = readYaml(outputDir.resolve("src/main/resources/application.yml"));
        assertEquals("${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}",
                mapAt(yaml, "kafka", "bootstrap").get("servers"));
        assertEquals("${HTTP_PORT:8080}", mapAt(yaml, "quarkus", "http").get("port"));
        String pom = Files.readString(outputDir.resolve("pom.xml"));
        assertTrue("generated Quarkus app must read application.yml",
                pom.contains("<artifactId>quarkus-config-yaml</artifactId>"));

        Map<String, Object> messaging = mapAt(yaml, "mp", "messaging");
        Map<String, Object> incoming = mapAt(messaging, "incoming");
        Map<String, Object> outgoing = mapAt(messaging, "outgoing");

        Set<String> overlap = new LinkedHashSet<>(incoming.keySet());
        overlap.retainAll(outgoing.keySet());
        assertTrue("SmallRye channel names must not be both incoming and outgoing: " + overlap, overlap.isEmpty());

        assertEquals("data_pipeline_demo_start",
                topicOf(incoming, "data_pipeline_demo_transform_data_in"));
        assertEquals("data_pipeline_demo_transform_data_output",
                topicOf(incoming, "data_pipeline_demo_filter_fields_in"));
        assertEquals("data_pipeline_demo_filter_fields_output",
                topicOf(incoming, "data_pipeline_demo_enrich_data_in"));
        assertEquals("data_pipeline_demo_transform_data_output",
                topicOf(outgoing, "data_pipeline_demo_transform_data_out"));
        assertEquals("data_pipeline_demo_transform_data_output",
                topicOf(incoming, "data_pipeline_demo_transform_data_output"));
    }

    @Test
    public void generationWritesNestedSubProcessProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes nested sub-process entry/completion Java files and application.yml channels");
        Path outputDir = Files.createTempDirectory("durga-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_nested_subprocess.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ReviewScopeSubProcessEntryService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ReviewScopeSubProcessCompletionService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ApprovalScopeSubProcessEntryService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ApprovalScopeSubProcessCompletionService.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_nested_subprocess_review_scope_input"));
        assertTrue(applicationYaml.contains("invoice_nested_subprocess_approval_scope_output"));
    }

    @Test
    public void generationWritesNonInterruptingBoundaryTimerWithoutCancellation() throws Exception {
        System.out.println("TC: generation of non-interrupting boundary timer does not include CANCELLED status check");
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-boundary-project-");

        runGeneration("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn", outputDir);

        Path boundaryHandler = outputDir.resolve("src/main/java/org/example/generated/ReviewReminderBoundaryTimerService.java");
        assertTrue(Files.exists(boundaryHandler));

        String boundarySource = Files.readString(boundaryHandler);
        assertTrue(boundarySource.contains("send_review_reminder"));
        assertFalse(boundarySource.contains("ProcessEvent.Status.CANCELLED"));
    }

    @Test
    public void generationWritesNonInterruptingSubProcessBoundaryTimerWithoutCancellation() throws Exception {
        System.out.println("TC: generation of non-interrupting sub-process boundary timer does not include CANCELLED status check");
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-subprocess-boundary-project-");

        runGeneration("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn", outputDir);

        Path boundaryHandler = outputDir.resolve("src/main/java/org/example/generated/ReviewScopeReminderBoundaryTimerService.java");
        assertTrue(Files.exists(boundaryHandler));

        String boundarySource = Files.readString(boundaryHandler);
        assertTrue(boundarySource.contains("notify_manager"));
        assertFalse(boundarySource.contains("ProcessEvent.Status.CANCELLED"));
    }

    @Test
    public void generationWritesEventSubProcessProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes event sub-process start/completion Java files and application.yml channel");
        Path outputDir = Files.createTempDirectory("durga-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ManagerNotificationScopeEventSubProcessStartService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/example/generated/ManagerNotificationScopeEventSubProcessCompletionService.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_event_subprocess_message_manager_note_message"));
    }

    @Test
    public void generationWritesInterruptingEventSubProcessCancellation() throws Exception {
        System.out.println("TC: generation of interrupting event sub-process includes CANCELLED status and cancels sibling activities");
        Path outputDir = Files.createTempDirectory("durga-interrupting-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/example/generated/InvoiceCancellationScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.CANCELLED"));
        assertTrue(source.contains("\"register_invoice\""));
        assertTrue(source.contains("\"review_invoice\""));
        assertTrue(source.contains("\"notify_requester\""));
    }

    @Test
    public void generationWritesTimerEventSubProcessProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes timer event sub-process start handler with delay configuration");
        Path outputDir = Files.createTempDirectory("durga-timer-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/example/generated/ReviewReminderScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("\"review_scope\""));
        assertTrue(source.contains("delayMillis(\"timeDuration\", \"PT10S\")"));
    }

    @Test
    public void generationWritesErrorEventSubProcessProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes error event sub-process start handler with FAILED status");
        Path outputDir = Files.createTempDirectory("durga-error-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/example/generated/ReviewErrorScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.FAILED"));
        assertTrue(source.contains("\"review_failed\""));
    }

    @Test
    public void generationWritesEscalationEventSubProcessProjectArtifacts() throws Exception {
        System.out.println("TC: generation writes escalation event sub-process start handler with ESCALATED status");
        Path outputDir = Files.createTempDirectory("durga-escalation-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/example/generated/ReviewEscalationScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.ESCALATED"));
        assertTrue(source.contains("\"review_blocked\""));
    }

    @Test
    public void generatedMessageEventProjectCompiles() throws Exception {
        System.out.println("TC: generated message event project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-message-compile-");

        runGeneration("src/test/resources/bpmn/invoice_message_exchange.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNestedSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated nested sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_nested_subprocess.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedBoundaryTimerProjectCompiles() throws Exception {
        System.out.println("TC: generated boundary timer project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-boundary-timer-compile-");

        runGeneration("src/test/resources/bpmn/invoice_review_deadline.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNonInterruptingBoundaryTimerProjectCompiles() throws Exception {
        System.out.println("TC: generated non-interrupting boundary timer project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-boundary-compile-");

        runGeneration("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNonInterruptingSubProcessBoundaryTimerProjectCompiles() throws Exception {
        System.out.println("TC: generated non-interrupting sub-process boundary timer project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-subprocess-boundary-compile-");

        runGeneration("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedEventSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated event sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedInterruptingEventSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated interrupting event sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-interrupting-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedTimerEventSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated timer event sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-timer-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedErrorEventSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated error event sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-error-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedEscalationEventSubProcessProjectCompiles() throws Exception {
        System.out.println("TC: generated escalation event sub-process project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-escalation-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedCallActivityProjectCompiles() throws Exception {
        System.out.println("TC: generated call activity project compiles and passes Maven test");
        Path outputDir = Files.createTempDirectory("durga-call-activity-compile-");

        runGeneration("src/test/resources/bpmn/invoice_call_activity.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedDataPipelineProjectCompilesWithLineageMetadata() throws Exception {
        System.out.println("TC: generated data pipeline project compiles with Vannak lineage metadata");
        Path outputDir = Files.createTempDirectory("durga-data-pipeline-compile-");

        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    private static Map<String, Object> readYaml(Path path) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), new TypeReference<>() {
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapAt(Map<String, Object> root, String... path) {
        Map<String, Object> current = root;
        for (String segment : path) {
            Object next = current.get(segment);
            assertTrue("Expected map at " + segment + " in " + current.keySet(), next instanceof Map);
            current = (Map<String, Object>) next;
        }
        return current;
    }

    private static String topicOf(Map<String, Object> channels, String channel) {
        Map<String, Object> config = mapAt(channels, channel);
        Object topic = config.get("topic");
        assertTrue("Missing topic for channel " + channel, topic instanceof String);
        return (String) topic;
    }

    private static Path fixturePath(String relativePath) {
        Path path = Path.of(relativePath);
        if (Files.exists(path)) {
            return path;
        }
        String prefix = "src/test/resources/";
        if (relativePath.startsWith(prefix)) {
            Path modulePath = Path.of("durga-tools", relativePath);
            if (Files.exists(modulePath)) {
                return modulePath;
            }
        }
        return path;
    }

    private static String runDryRun(String relativeBpmnPath) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            BpmnScaffolder.main(new String[]{
                    "--dry-run",
                    fixturePath(relativeBpmnPath).toAbsolutePath().toString()
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private static void runGeneration(String relativeBpmnPath, Path outputDir) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            BpmnScaffolder.main(new String[]{
                    fixturePath(relativeBpmnPath).toAbsolutePath().toString(),
                    "--out",
                    outputDir.toAbsolutePath().toString()
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
    }

    private static void runRustGeneration(String relativeBpmnPath, Path outputDir, boolean validation) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            java.util.List<String> args = new java.util.ArrayList<>();
            args.add(fixturePath(relativeBpmnPath).toAbsolutePath().toString());
            args.add("--out");
            args.add(outputDir.toAbsolutePath().toString());
            args.add("--target");
            args.add("rust");
            if (validation) {
                args.add("--validation");
            }
            BpmnScaffolder.main(args.toArray(new String[0]));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
    }

    private static void runGenerationWithValidation(String relativeBpmnPath, Path outputDir) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            BpmnScaffolder.main(new String[]{
                    fixturePath(relativeBpmnPath).toAbsolutePath().toString(),
                    "--out",
                    outputDir.toAbsolutePath().toString(),
                    "--validation"
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
    }

    private static void runGeneratedMavenTest(Path outputDir) throws Exception {
        ensureDurgaRuntimeInstalled();
        Process process = new ProcessBuilder("mvn", "-q", "test")
                .directory(outputDir.toFile())
                .redirectErrorStream(true)
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        process.getInputStream().transferTo(captured);

        boolean finished = process.waitFor(3, TimeUnit.MINUTES);
        assertTrue("Generated project Maven test timed out for " + outputDir, finished);
        assertEquals(
                "Generated project Maven test failed for " + outputDir + "\n" + captured.toString(StandardCharsets.UTF_8),
                0,
                process.exitValue()
        );
    }

    private static void runGeneratedMavenPackage(Path outputDir) throws Exception {
        ensureDurgaRuntimeInstalled();
        Process process = new ProcessBuilder("mvn", "-q", "package")
                .directory(outputDir.toFile())
                .redirectErrorStream(true)
                .start();

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        process.getInputStream().transferTo(captured);

        boolean finished = process.waitFor(3, TimeUnit.MINUTES);
        assertTrue("Generated project Maven package timed out for " + outputDir, finished);
        assertEquals(
                "Generated project Maven package failed for " + outputDir + "\n" + captured.toString(StandardCharsets.UTF_8),
                0,
                process.exitValue()
        );
    }

    private static volatile boolean durgaRuntimeInstalled = false;

    /**
     * Generated projects depend on {@code org.gautelis:durga-runtime} for the concrete plugin
     * classes, so the spawned generated-project Maven builds must resolve that artifact from the
     * local repository. A plain reactor {@code mvn test}/{@code verify} builds durga-runtime but
     * never installs it, so these tests only pass on a warm {@code ~/.m2}. Install it once here so
     * the tests are self-contained on a cold local repository (and in CI).
     */
    private static synchronized void ensureDurgaRuntimeInstalled() throws Exception {
        if (durgaRuntimeInstalled) {
            return;
        }
        Path repoRoot = findRepoRoot();
        Process process = new ProcessBuilder(
                "mvn", "-q", "-pl", "durga-runtime", "install",
                "-DskipTests", "-Dcheckstyle.skip=true", "-Dspotbugs.skip=true", "-Djacoco.skip=true")
                .directory(repoRoot.toFile())
                .redirectErrorStream(true)
                .start();
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        process.getInputStream().transferTo(captured);
        boolean finished = process.waitFor(5, TimeUnit.MINUTES);
        assertTrue("durga-runtime install timed out\n" + captured.toString(StandardCharsets.UTF_8), finished);
        assertEquals(
                "Failed to install durga-runtime to the local repository\n"
                        + captured.toString(StandardCharsets.UTF_8),
                0,
                process.exitValue());
        durgaRuntimeInstalled = true;
    }

    private static Path findRepoRoot() {
        Path dir = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++) {
            if (Files.exists(dir.resolve("durga-runtime/pom.xml"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
                "Could not locate the repository root (durga-runtime module) from "
                        + Path.of("").toAbsolutePath());
    }

    private static String extractCustomHash(String bpmn) {
        String marker = "name=\"customHash\" value=\"";
        int start = bpmn.indexOf(marker);
        assertTrue("customHash property missing", start >= 0);
        start += marker.length();
        int end = bpmn.indexOf('"', start);
        assertTrue("customHash property value not terminated", end > start);
        return bpmn.substring(start, end);
    }

    @Test
    public void dryRunIncludesPluginExecutorArtifacts() throws Exception {
        System.out.println("TC: dry-run generates plugin executor classes for data pipeline tasks");
        String output = runDryRun("src/test/resources/bpmn/data_pipeline_demo.bpmn");
        assertTrue(output.contains("Tasks: [transform_data, filter_fields, enrich_data]"));
        assertTrue(output.contains("Data objects: [raw_customer_data, normalized_customer_data, filtered_customer_data, enriched_customer_data"));
        assertTrue(output.contains("Data stores: [s3_warehouse, neo4j_customer_graph, postgresql_customer_store]"));

        /*
        Cannot use:
        assertTrue(output.contains("TransformDataPluginExecutor"));
        assertTrue(output.contains("FilterFieldsPluginExecutor"));
        assertTrue(output.contains("EnrichDataPluginExecutor"));

        when data looks like:
            ➜  durga git:(main) java -jar target/durga-0.1.0-beta.1.jar src/test/resources/bpmn/data_pipeline_demo.bpmn
            Note: plugin 'json-schema-validator' has status 'experimental'
            Note: plugin 'kv-enricher' has status 'experimental'
            Note: plugin 'field-router' has status 'experimental'
            Note: plugin 'window-counter' has status 'experimental'
            <string> 598:92: doesn't look like an expression
            Generated in /Users/froran/Projects/gautelis/durga/generated
            Process: data_pipeline_demo
            Tasks: [transform_data, filter_fields, enrich_data]
            Call activities: []
            Embedded subprocesses: []
            Data objects: [raw_customer_data, normalized_customer_data, filtered_customer_data, enriched_customer_data]
            Data stores: [s3_warehouse, neo4j_customer_graph, postgresql_customer_store]
            OR gateways: []
            Multi-instance tasks: []
            Event subprocesses: []
            Timers: []
            Boundary events: []
            Message events: []
            Signal events: []
            XOR gateways: []
            AND gateways: []
         */
    }

    @Test
    public void generatedDataPipelineProjectContainsPluginExecutors() throws Exception {        System.out.println("TC: generated data pipeline project contains plugin executor with plugin import and execution");
        Path outputDir = Files.createTempDirectory("durga-pipeline-test-");
        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);
        Path transformFile = outputDir.resolve(
                "src/main/java/org/example/generated/TransformDataPluginExecutor.java");
        assertTrue(Files.exists(transformFile));
        String content = Files.readString(transformFile);
        assertTrue(content.contains("import org.gautelis.durga.plugins.Plugin"));
        assertTrue(content.contains("plugin.executeWithResult("));
        assertTrue(content.contains("outputPayload"));
        assertTrue(content.contains("mapper.readTree(outputText)"));
        assertTrue(content.contains("outputPayload,"));
        assertTrue(content.contains("@Channel(\"vannak-metadata-events\")"));
        assertTrue(content.contains("VannakMetadata.pluginEvent("));

        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/gautelis/durga/DataIndividualMetadataEvent.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/gautelis/durga/VannakMetadata.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("vannak-metadata-events"));
    }

    @Test
    public void validationModeGeneratesShadowWorkersChannelsAndTopics() throws Exception {
        System.out.println("TC: --validation generates shadow workers, dedicated validation channels, and topics for plugin tasks");
        Path outputDir = Files.createTempDirectory("durga-validation-test-");
        runGenerationWithValidation("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);

        Path shadow = outputDir.resolve(
                "src/main/java/org/example/generated/TransformDataValidationWorker.java");
        assertTrue("shadow worker not generated", Files.exists(shadow));
        String shadowContent = Files.readString(shadow);
        assertTrue(shadowContent.contains("@Incoming(\"data_pipeline_demo_transform_data_validation_in\")"));
        assertTrue(shadowContent.contains("@Channel(\"validation-candidate-outputs\")"));
        assertTrue(shadowContent.contains("ValidationCandidateOutput"));
        assertTrue(shadowContent.contains("plugin.executeWithResult("));
        assertFalse("shadow worker must not emit lifecycle events", shadowContent.contains("processEventsEmitter"));
        assertFalse("shadow worker must not emit to the task output channel", shadowContent.contains("_out\")"));

        assertTrue("ValidationCandidateOutput runtime type not generated", Files.exists(outputDir.resolve(
                "src/main/java/org/gautelis/durga/validation/ValidationCandidateOutput.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("data_pipeline_demo_transform_data_validation_in"));
        assertTrue(applicationYaml.contains("validation-candidate-outputs"));
        assertTrue("dedicated consumer group missing",
                applicationYaml.contains("data_pipeline_demo_transform_data_validation"));
        assertTrue("configurable offset reset missing", applicationYaml.contains("DURGA_VALIDATION_OFFSET_RESET"));

        String topics = Files.readString(outputDir.resolve("topics.sh"));
        assertTrue(topics.contains("validation-candidate-outputs"));
        assertTrue(topics.contains("validation-results"));
    }

    @Test
    public void rustValidationTargetGeneratesShadowBinsAndRuntime() throws Exception {
        System.out.println("TC: --target rust --validation generates shadow worker bins and validation runtime glue");
        Path outputDir = Files.createTempDirectory("durga-rust-validation-test-");
        runRustGeneration("src/test/resources/bpmn/e2e_pipeline.bpmn", outputDir, true);

        Path shadowBin = outputDir.resolve("src/bin/transform_order_validation.rs");
        assertTrue("rust shadow worker bin not generated", Files.exists(shadowBin));
        String binContent = Files.readString(shadowBin);
        assertTrue(binContent.contains("run_validation_worker"));
        assertTrue(binContent.contains("ValidationWorker"));
        assertTrue(binContent.contains("e2e_pipeline-transform_order-validation"));
        assertTrue(binContent.contains("DURGA_VALIDATION_CANDIDATE_VERSION"));

        String lib = Files.readString(outputDir.resolve("src/lib.rs"));
        assertTrue(lib.contains("pub async fn run_validation_worker"));
        assertTrue(lib.contains("validation-candidate-outputs"));
        assertTrue(lib.contains("DURGA_VALIDATION_OFFSET_RESET"));

        assertFalse("gateway must not get a validation bin",
                Files.exists(outputDir.resolve("src/bin/route_by_amount_validation.rs")));
    }

    @Test
    public void rustDefaultTargetHasNoValidationArtifacts() throws Exception {
        System.out.println("TC: rust generation without --validation produces no shadow bins or validation runtime");
        Path outputDir = Files.createTempDirectory("durga-rust-no-validation-test-");
        runRustGeneration("src/test/resources/bpmn/e2e_pipeline.bpmn", outputDir, false);

        assertFalse(Files.exists(outputDir.resolve("src/bin/transform_order_validation.rs")));
        String lib = Files.readString(outputDir.resolve("src/lib.rs"));
        assertFalse(lib.contains("run_validation_worker"));
        assertFalse(lib.contains("validation-candidate-outputs"));
    }

    @Test
    public void defaultGenerationHasNoValidationArtifacts() throws Exception {
        System.out.println("TC: generation without --validation produces no shadow workers or validation topics");
        Path outputDir = Files.createTempDirectory("durga-no-validation-test-");
        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);

        assertFalse(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/TransformDataValidationWorker.java")));
        assertFalse(Files.exists(outputDir.resolve(
                "src/main/java/org/gautelis/durga/validation/ValidationCandidateOutput.java")));
        String topics = Files.readString(outputDir.resolve("topics.sh"));
        assertFalse(topics.contains("validation-candidate-outputs"));
        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertFalse(applicationYaml.contains("validation-candidate-outputs"));
    }

    @Test
    public void generatedDataPipelineProjectContainsDataAssetMetadata() throws Exception {
        System.out.println("TC: generated data pipeline project contains data object and store metadata");
        Path outputDir = Files.createTempDirectory("durga-pipeline-data-test-");
        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);

        String summary = Files.readString(outputDir.resolve("summary.json"));
        assertTrue(summary.contains("\"dataObjects\""));
        assertTrue(summary.contains("\"name\" : \"normalized_customer_data\""));
        assertTrue(summary.contains("\"schema\" : \"schemas/normalized-customer-data.schema.json\""));
        assertTrue(summary.contains("\"dataStores\""));
        assertTrue(summary.contains("\"kind\" : \"s3\""));
        assertTrue(summary.contains("\"uri\" : \"s3://warehouse/customer/enriched/\""));
        assertTrue(summary.contains("\"target\" : \"neo4j_customer_graph\""));

        String readme = Files.readString(outputDir.resolve("README.md"));
        assertTrue(readme.contains("## Data Objects"));
        assertTrue(readme.contains("normalized_customer_data"));
        assertTrue(readme.contains("## Data Stores"));
        assertTrue(readme.contains("s3_warehouse kind=s3"));

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/DataHandle.java")));
    }

    @Test
    public void dryRunIncludesConnectArtifacts() throws Exception {
        System.out.println("TC: dry-run with --connect shows source and sink configs with populated topic lists");
        String output = runDryRunWithConnect("src/test/resources/bpmn/order_events_pipeline.bpmn");
        assertTrue("connect-source.json missing", output.contains("connect-source.json"));
        assertTrue("connect-sink.json missing", output.contains("connect-sink.json"));
        assertTrue("source topics not populated", output.contains("order_events_pipeline_start"));
        assertTrue("sink topics not populated", output.contains("process-events"));
        assertTrue("connector class placeholder missing", output.contains("<fill in connector class>"));
    }

    @Test
    public void generationWritesConnectFiles() throws Exception {
        System.out.println("TC: --connect generates source/sink configs and deploy script with correct topic lists");
        Path outputDir = Files.createTempDirectory("durga-connect-test-");
        runGenerationWithConnect("src/test/resources/bpmn/order_events_pipeline.bpmn", outputDir);

        Path sourceJson = outputDir.resolve("connect/connect-source.json");
        Path sinkJson = outputDir.resolve("connect/connect-sink.json");
        Path connectScript = outputDir.resolve("connect.sh");

        assertTrue("connect-source.json not created", Files.exists(sourceJson));
        assertTrue("connect-sink.json not created", Files.exists(sinkJson));
        assertTrue("connect.sh not created", Files.exists(connectScript));

        String sourceContent = Files.readString(sourceJson);
        assertTrue("source missing start topic", sourceContent.contains("order_events_pipeline_start"));
        assertTrue("source missing connector.class", sourceContent.contains("connector.class"));

        String sinkContent = Files.readString(sinkJson);
        assertTrue("sink missing process-events", sinkContent.contains("process-events"));
        assertTrue("sink missing terminal output topic",
                sinkContent.contains("order_events_pipeline_normalize_timestamp_output")
                || sinkContent.contains("order_events_pipeline_mask_customer_email_low_value_output"));
        assertTrue("sink missing connector.class", sinkContent.contains("connector.class"));
    }

    @Test
    public void connectGenerationWritesDataStoreConnectorSkeletons() throws Exception {
        System.out.println("TC: --connect generates connector skeletons for BPMN data stores");
        Path outputDir = Files.createTempDirectory("durga-data-store-connect-test-");
        runGenerationWithConnect("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);

        Path s3Sink = outputDir.resolve("connect/data-stores/data_pipeline_demo-s3-warehouse-sink.json");
        Path neo4jSink = outputDir.resolve("connect/data-stores/data_pipeline_demo-neo4j-customer-graph-sink.json");
        Path postgresSink = outputDir.resolve("connect/data-stores/data_pipeline_demo-postgresql-customer-store-sink.json");

        assertTrue("S3 data-store connector not created", Files.exists(s3Sink));
        assertTrue("Neo4j data-store connector not created", Files.exists(neo4jSink));
        assertTrue("PostgreSQL data-store connector not created", Files.exists(postgresSink));

        String s3Content = Files.readString(s3Sink);
        assertTrue(s3Content.contains("io.confluent.connect.s3.S3SinkConnector"));
        assertTrue(s3Content.contains("data_pipeline_demo_enrich_data_output"));
        assertTrue(s3Content.contains("s3://warehouse/customer/enriched/"));

        String neo4jContent = Files.readString(neo4jSink);
        assertTrue(neo4jContent.contains("streams.kafka.connect.sink.Neo4jSinkConnector"));
        assertTrue(neo4jContent.contains("neo4j://customer-graph"));

        String postgresContent = Files.readString(postgresSink);
        assertTrue(postgresContent.contains("io.confluent.connect.jdbc.JdbcSinkConnector"));
        assertTrue(postgresContent.contains("jdbc:postgresql://postgres/customers"));

        String script = Files.readString(outputDir.resolve("connect.sh"));
        assertTrue(script.contains("connect/data-stores/*.json"));
    }

    @Test
    public void generationWritesDataPipelineProject() throws Exception {
        System.out.println("TC: order events pipeline generates workers for all 8 plugin-annotated tasks");
        Path outputDir = Files.createTempDirectory("durga-order-pipeline-");
        runGeneration("src/test/resources/bpmn/order_events_pipeline.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/NormalizeOrderPluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/CoerceTypesPluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/EnrichHighValuePluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/XorRouteByAmountService.java")));

        Path xorFile = outputDir.resolve(
                "src/main/java/org/example/generated/XorRouteByAmountService.java");
        String xorContent = Files.readString(xorFile);
        assertTrue("XOR missing amount > 1000 condition",
                xorContent.contains("amount > 1000") || xorContent.contains("> 1000"));

        Path validateHighValueFile = outputDir.resolve(
                "src/main/java/org/example/generated/ValidateHighValuePluginExecutor.java");
        assertTrue(Files.exists(validateHighValueFile));
        String validateHighValueContent = Files.readString(validateHighValueFile);
        assertTrue("validate executor missing ACTIVITY_ESCALATED", validateHighValueContent.contains("ProcessEvent.EventType.ACTIVITY_ESCALATED"));
        assertTrue("validate executor missing ESCALATED status", validateHighValueContent.contains("ProcessEvent.Status.ESCALATED"));
        assertTrue("validate executor missing VALIDATION_FAILED error code", validateHighValueContent.contains("VALIDATION_FAILED"));
    }

    @Test
    public void generationWritesLogProcessingProject() throws Exception {
        System.out.println("TC: log processing pipeline generates workers for all plugin types");
        Path outputDir = Files.createTempDirectory("durga-log-pipeline-");
        runGeneration("src/test/resources/bpmn/log_processing_pipeline.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/ExtractLogFieldsPluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/CoerceLogTypesPluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/FormatMessagePluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/FlattenForIndexPluginExecutor.java")));
        assertTrue(Files.exists(outputDir.resolve(
                "src/main/java/org/example/generated/MaskIpAddressPluginExecutor.java")));

        Path validateFile = outputDir.resolve(
                "src/main/java/org/example/generated/ValidateLogRecordPluginExecutor.java");
        assertTrue(Files.exists(validateFile));
        String validateContent = Files.readString(validateFile);
        assertTrue("validate executor missing ACTIVITY_ESCALATED", validateContent.contains("ProcessEvent.EventType.ACTIVITY_ESCALATED"));
        assertTrue("validate executor missing ESCALATED status", validateContent.contains("ProcessEvent.Status.ESCALATED"));
        assertTrue("validate executor missing VALIDATION_FAILED error code", validateContent.contains("VALIDATION_FAILED"));
    }

    @Test
    public void generatedProjectPackageEnrichesEmbeddedBpmnWithLocalCustomImplementation() throws Exception {
        System.out.println("TC: generated log processing project package enriches embedded BPMN with local custom implementation metadata");
        Path workDir = Files.createTempDirectory("durga-log-custom-roundtrip-");
        Path sourceBpmn = workDir.resolve("log_processing_pipeline.bpmn");
        String model = Files.readString(fixturePath("src/test/resources/bpmn/log_processing_pipeline.bpmn"));
        model = model.replaceAll("(?s)\\s+<bpmn:extensionElements>.*?</bpmn:extensionElements>", "");
        model = model.replace(
                "    <bpmn:serviceTask id=\"mask_ip\" name=\"Mask IP Address\">\n",
                """
                    <bpmn:serviceTask id="mask_ip" name="Mask IP Address">
                      <bpmn:extensionElements>
                        <camunda:properties>
                          <camunda:property name="plugin" value="custom" />
                          <camunda:property name="pluginConfig" value="interface=org.example.generated.MaskIpAddressContract" />
                        </camunda:properties>
                      </bpmn:extensionElements>
                """
        );
        Files.writeString(sourceBpmn, model, StandardCharsets.UTF_8);

        Path outputDir = workDir.resolve("generated");
        runGeneration(sourceBpmn.toString(), outputDir);

        Path implFile = outputDir.resolve(
                "src/main/java/org/example/generated/MaskIpAddressLocalImplementation.java");
        Files.writeString(implFile, """
                package org.example.generated;

                import jakarta.enterprise.context.ApplicationScoped;

                @ApplicationScoped
                public class MaskIpAddressLocalImplementation implements MaskIpAddressContract {
                    public String execute(String payload, String config) {
                        return payload.replace("127.0.0.1", "masked");
                    }
                }
                """, StandardCharsets.UTF_8);

        runGeneratedMavenPackage(outputDir);

        Path embeddedModel = outputDir.resolve("src/main/resources/log_processing_pipeline.bpmn");
        String enriched = Files.readString(embeddedModel, StandardCharsets.UTF_8);
        assertTrue("customImpl not written to embedded BPMN",
                enriched.contains("org.example.generated.MaskIpAddressLocalImplementation"));
        assertTrue("customSource not written to embedded BPMN",
                enriched.contains("MaskIpAddressLocalImplementation.java"));
        assertTrue("customHash not written to embedded BPMN",
                enriched.contains("customHash"));
        String firstHash = extractCustomHash(enriched);

        Files.writeString(implFile, """
                package org.example.generated;

                import jakarta.enterprise.context.ApplicationScoped;

                @ApplicationScoped
                public class MaskIpAddressLocalImplementation implements MaskIpAddressContract {
                    public String execute(String payload, String config) {
                        return payload.replace("127.0.0.1", "masked-again");
                    }
                }
                """, StandardCharsets.UTF_8);

        runGeneratedMavenPackage(outputDir);

        String reEnriched = Files.readString(embeddedModel, StandardCharsets.UTF_8);
        String secondHash = extractCustomHash(reEnriched);
        assertFalse("customHash did not change after local source modification", firstHash.equals(secondHash));

        Path regeneratedDir = workDir.resolve("regenerated");
        runGeneration(embeddedModel.toString(), regeneratedDir);

        Path regeneratedModel = regeneratedDir.resolve("src/main/resources/log_processing_pipeline.bpmn");
        String regeneratedBpmn = Files.readString(regeneratedModel, StandardCharsets.UTF_8);
        assertTrue("regenerated BPMN lost customImpl",
                regeneratedBpmn.contains("org.example.generated.MaskIpAddressLocalImplementation"));
        assertTrue("regenerated BPMN lost customSource",
                regeneratedBpmn.contains("MaskIpAddressLocalImplementation.java"));
        assertTrue("regenerated BPMN lost latest customHash",
                regeneratedBpmn.contains(secondHash));

        Path regeneratedWorker = regeneratedDir.resolve(
                "src/main/java/org/example/generated/MaskIpAddressWorkerService.java");
        String regeneratedWorkerSource = Files.readString(regeneratedWorker, StandardCharsets.UTF_8);
        assertTrue("regenerated worker lost expected implementation metadata",
                regeneratedWorkerSource.contains("MaskIpAddressLocalImplementation"));
        assertTrue("regenerated worker lost expected implementation hash",
                regeneratedWorkerSource.contains(secondHash));
    }

    private static String runDryRunWithConnect(String relativeBpmnPath) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            BpmnScaffolder.main(new String[]{
                    "--dry-run",
                    "--connect",
                    fixturePath(relativeBpmnPath).toAbsolutePath().toString()
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private static void runGenerationWithConnect(String relativeBpmnPath, Path outputDir) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream captureStream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        try {
            System.setOut(captureStream);
            System.setErr(captureStream);
            BpmnScaffolder.main(new String[]{
                    fixturePath(relativeBpmnPath).toAbsolutePath().toString(),
                    "--out",
                    outputDir.toAbsolutePath().toString(),
                    "--connect"
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
    }

    @Test
    public void generatedProjectCompiles() throws Exception {
        System.out.println("TC: generated project compiles without errors");
        Path outputDir = Files.createTempDirectory("durga-compile-test-");

        runGeneration("src/test/resources/bpmn/invoice_receipt.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }
}
