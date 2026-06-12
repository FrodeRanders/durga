package org.gautelis.durga.tools;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BpmnScaffolderTest {
    @Test
    public void dryRunIncludesMessageEventArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_message_exchange.bpmn");

        assertTrue(output.contains("Process: invoice_message_exchange"));
        assertTrue(output.contains("Message events: [await_review_response, send_review_request]"));
        assertTrue(output.contains("AwaitReviewResponseMessageCatchService"));
        assertTrue(output.contains("SendReviewRequestMessageThrowService"));
        assertTrue(output.contains("invoice_message_exchange_invoice_review_response_message"));
    }

    @Test
    public void dryRunIncludesNestedSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_nested_subprocess.bpmn");

        assertTrue(output.contains("Embedded subprocesses: [review_scope, approval_scope]"));
        assertTrue(output.contains("ReviewScopeSubProcessEntryService"));
        assertTrue(output.contains("ReviewScopeSubProcessCompletionService"));
        assertTrue(output.contains("ApprovalScopeSubProcessEntryService"));
        assertTrue(output.contains("ApprovalScopeSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesSubProcessBoundaryErrorArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_subprocess_error.bpmn");

        assertTrue(output.contains("Boundary events: [review_scope_error]"));
        assertTrue(output.contains("ReviewScopeErrorBoundaryErrorService"));
        assertTrue(output.contains("ReviewScopeSubProcessEntryService"));
        assertTrue(output.contains("ReviewScopeSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesTimerArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_receipt_reminder.bpmn");

        assertTrue(output.contains("Timers: [wait_before_notify]"));
        assertTrue(output.contains("WaitBeforeNotifyTimerService"));
        assertTrue(output.contains("invoice_receipt_reminder_wait_before_notify_input"));
    }

    @Test
    public void dryRunIncludesBoundaryEscalationArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_review_escalation.bpmn");

        assertTrue(output.contains("Boundary events: [review_escalation]"));
        assertTrue(output.contains("ReviewEscalationBoundaryEscalationService"));
        assertTrue(output.contains("InvoiceReviewEscalationTaskEscalationPublisher"));
    }

    @Test
    public void dryRunIncludesNonInterruptingBoundaryTimerArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn");

        assertTrue(output.contains("Boundary events: [review_reminder]"));
        assertTrue(output.contains("ReviewReminderBoundaryTimerService"));
        assertTrue(output.contains("SendReviewReminderWorkerService"));
    }

    @Test
    public void dryRunIncludesNonInterruptingSubProcessBoundaryTimerArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn");

        assertTrue(output.contains("Boundary events: [review_scope_reminder]"));
        assertTrue(output.contains("Embedded subprocesses: [review_scope]"));
        assertTrue(output.contains("ReviewScopeReminderBoundaryTimerService"));
        assertTrue(output.contains("NotifyManagerWorkerService"));
    }

    @Test
    public void dryRunIncludesSignalArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_signal_exchange.bpmn");

        assertTrue(output.contains("Signal events: [await_review_signal, send_review_signal]"));
        assertTrue(output.contains("AwaitReviewSignalSignalCatchService"));
        assertTrue(output.contains("SendReviewSignalSignalThrowService"));
        assertTrue(output.contains("invoice_signal_exchange_invoice_review_signal_signal"));
    }

    @Test
    public void dryRunIncludesCallActivityArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_call_activity.bpmn");

        assertTrue(output.contains("Call activities: [validate_invoice_process]"));
        assertTrue(output.contains("ValidateInvoiceProcessCallActivityService"));
        assertTrue(output.contains("InvoiceCallActivityCallActivityCompletionPublisher"));
        assertTrue(output.contains("complete-call-activity.sh"));
    }

    @Test
    public void dryRunIncludesEventSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn");

        assertTrue(output.contains("Event subprocesses: [manager_notification_scope]"));
        assertTrue(output.contains("ManagerNotificationScopeEventSubProcessStartService"));
        assertTrue(output.contains("ManagerNotificationScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("invoice_event_subprocess_message_manager_note_message"));
    }

    @Test
    public void dryRunIncludesInterruptingEventSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn");

        assertTrue(output.contains("Event subprocesses: [invoice_cancellation_scope]"));
        assertTrue(output.contains("InvoiceCancellationScopeEventSubProcessStartService"));
        assertTrue(output.contains("InvoiceCancellationScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("invoice_event_subprocess_interrupting_message_cancel_invoice_message"));
    }

    @Test
    public void dryRunIncludesTimerEventSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_reminder_scope]"));
        assertTrue(output.contains("ReviewReminderScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewReminderScopeEventSubProcessCompletionService"));
        assertTrue(output.contains("Embedded subprocesses: [review_scope]"));
    }

    @Test
    public void dryRunIncludesErrorEventSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_error_scope]"));
        assertTrue(output.contains("ReviewErrorScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewErrorScopeEventSubProcessCompletionService"));
    }

    @Test
    public void dryRunIncludesEscalationEventSubProcessArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn");

        assertTrue(output.contains("Event subprocesses: [review_escalation_scope]"));
        assertTrue(output.contains("ReviewEscalationScopeEventSubProcessStartService"));
        assertTrue(output.contains("ReviewEscalationScopeEventSubProcessCompletionService"));
    }

    @Test
    public void generationWritesMessageEventProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-message-project-");

        runGeneration("src/test/resources/bpmn/invoice_message_exchange.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/AwaitReviewResponseMessageCatchService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/SendReviewRequestMessageThrowService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/InvoiceMessageExchangeMessageEventPublisher.java")));
        assertTrue(Files.exists(outputDir.resolve("send-message-event.sh")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_message_exchange_invoice_review_response_message"));
        assertTrue(applicationYaml.contains("invoice_message_exchange_invoice_review_request_message"));
    }

    @Test
    public void generationWritesNestedSubProcessProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_nested_subprocess.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewScopeSubProcessEntryService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewScopeSubProcessCompletionService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ApprovalScopeSubProcessEntryService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ApprovalScopeSubProcessCompletionService.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_nested_subprocess_review_scope_input"));
        assertTrue(applicationYaml.contains("invoice_nested_subprocess_approval_scope_output"));
    }

    @Test
    public void generationWritesNonInterruptingBoundaryTimerWithoutCancellation() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-boundary-project-");

        runGeneration("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn", outputDir);

        Path boundaryHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewReminderBoundaryTimerService.java");
        assertTrue(Files.exists(boundaryHandler));

        String boundarySource = Files.readString(boundaryHandler);
        assertTrue(boundarySource.contains("send_review_reminder"));
        assertFalse(boundarySource.contains("ProcessEvent.Status.CANCELLED"));
    }

    @Test
    public void generationWritesNonInterruptingSubProcessBoundaryTimerWithoutCancellation() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-subprocess-boundary-project-");

        runGeneration("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn", outputDir);

        Path boundaryHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewScopeReminderBoundaryTimerService.java");
        assertTrue(Files.exists(boundaryHandler));

        String boundarySource = Files.readString(boundaryHandler);
        assertTrue(boundarySource.contains("notify_manager"));
        assertFalse(boundarySource.contains("ProcessEvent.Status.CANCELLED"));
    }

    @Test
    public void generationWritesEventSubProcessProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn", outputDir);

        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ManagerNotificationScopeEventSubProcessStartService.java")));
        assertTrue(Files.exists(outputDir.resolve("src/main/java/org/gautelis/durga/generated/ManagerNotificationScopeEventSubProcessCompletionService.java")));

        String applicationYaml = Files.readString(outputDir.resolve("src/main/resources/application.yml"));
        assertTrue(applicationYaml.contains("invoice_event_subprocess_message_manager_note_message"));
    }

    @Test
    public void generationWritesInterruptingEventSubProcessCancellation() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-interrupting-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/InvoiceCancellationScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.CANCELLED"));
        assertTrue(source.contains("\"register_invoice\""));
        assertTrue(source.contains("\"review_invoice\""));
        assertTrue(source.contains("\"notify_requester\""));
    }

    @Test
    public void generationWritesTimerEventSubProcessProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-timer-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewReminderScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("\"review_scope\""));
        assertTrue(source.contains("delayMillis(\"timeDuration\", \"PT10S\")"));
    }

    @Test
    public void generationWritesErrorEventSubProcessProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-error-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewErrorScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.FAILED"));
        assertTrue(source.contains("\"review_failed\""));
    }

    @Test
    public void generationWritesEscalationEventSubProcessProjectArtifacts() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-escalation-event-subprocess-project-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn", outputDir);

        Path startHandler = outputDir.resolve("src/main/java/org/gautelis/durga/generated/ReviewEscalationScopeEventSubProcessStartService.java");
        assertTrue(Files.exists(startHandler));

        String source = Files.readString(startHandler);
        assertTrue(source.contains("ProcessEvent.Status.ESCALATED"));
        assertTrue(source.contains("\"review_blocked\""));
    }

    @Test
    public void generatedMessageEventProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-message-compile-");

        runGeneration("src/test/resources/bpmn/invoice_message_exchange.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNestedSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_nested_subprocess.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedBoundaryTimerProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-boundary-timer-compile-");

        runGeneration("src/test/resources/bpmn/invoice_review_deadline.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNonInterruptingBoundaryTimerProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-boundary-compile-");

        runGeneration("src/test/resources/bpmn/invoice_review_reminder_non_interrupting.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedNonInterruptingSubProcessBoundaryTimerProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-non-interrupting-subprocess-boundary-compile-");

        runGeneration("src/test/resources/bpmn/invoice_subprocess_reminder_non_interrupting.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedEventSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_message.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedInterruptingEventSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-interrupting-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_interrupting_message.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedTimerEventSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-timer-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_timer.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedErrorEventSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-error-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_error.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedEscalationEventSubProcessProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-escalation-event-subprocess-compile-");

        runGeneration("src/test/resources/bpmn/invoice_event_subprocess_escalation.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
    }

    @Test
    public void generatedCallActivityProjectCompiles() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-call-activity-compile-");

        runGeneration("src/test/resources/bpmn/invoice_call_activity.bpmn", outputDir);
        runGeneratedMavenTest(outputDir);
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
                    Path.of(relativeBpmnPath).toAbsolutePath().toString()
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
                    Path.of(relativeBpmnPath).toAbsolutePath().toString(),
                    "--out",
                    outputDir.toAbsolutePath().toString()
            });
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureStream.close();
        }
    }

    private static void runGeneratedMavenTest(Path outputDir) throws Exception {
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

    @Test
    public void dryRunIncludesPluginExecutorArtifacts() throws Exception {
        String output = runDryRun("src/test/resources/bpmn/data_pipeline_demo.bpmn");
        assertTrue(output.contains("Tasks: [transform_data, filter_fields, enrich_data]"));
        assertTrue(output.contains("TransformDataPluginExecutor"));
        assertTrue(output.contains("FilterFieldsPluginExecutor"));
        assertTrue(output.contains("EnrichDataPluginExecutor"));
    }

    @Test
    public void generatedDataPipelineProjectContainsPluginExecutors() throws Exception {
        Path outputDir = Files.createTempDirectory("durga-pipeline-test-");
        runGeneration("src/test/resources/bpmn/data_pipeline_demo.bpmn", outputDir);
        Path transformFile = outputDir.resolve(
                "src/main/java/org/gautelis/durga/generated/TransformDataPluginExecutor.java");
        assertTrue(Files.exists(transformFile));
        String content = Files.readString(transformFile);
        assertTrue(content.contains("import org.gautelis.durga.plugins.Plugin"));
        assertTrue(content.contains("plugin.execute(payload"));
    }
}
