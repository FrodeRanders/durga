package org.gautelis.durga.tools;

import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class NameValidationTest {

    private static BpmnModelInstance model(String innerProcessXml) {
        String xml = "<?xml version='1.0' encoding='UTF-8'?>"
                + "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" "
                + "targetNamespace=\"http://example.com/bpmn\">" + innerProcessXml + "</definitions>";
        return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private static boolean hasError(List<NameValidation.Issue> issues, String fragment) {
        return issues.stream().anyMatch(i -> i.severity() == NameValidation.Severity.ERROR
                && i.message().contains(fragment));
    }

    private static boolean hasWarning(List<NameValidation.Issue> issues, String fragment) {
        return issues.stream().anyMatch(i -> i.severity() == NameValidation.Severity.WARNING
                && i.message().contains(fragment));
    }

    @Test
    public void shouldWarnButNotErrorForTransliteratedSwedishNames() {
        System.out.println("TC: Swedish names transliterate cleanly with warnings and no errors");
        BpmnModelInstance model = model(
                "<process id=\"återköp\" name=\"Återköpsflöde\">"
                        + "<serviceTask id=\"registrera_arende\" name=\"Registrera ärende\" />"
                        + "<userTask id=\"bedom\" name=\"Bedöm återköp\" />"
                        + "</process>");

        List<NameValidation.Issue> issues = NameValidation.validate(model, "återköp");

        assertFalse("Swedish names should not produce errors", NameValidation.hasErrors(issues));
        assertTrue(hasWarning(issues, "transliterated"));
    }

    @Test
    public void shouldErrorOnNameCollision() {
        System.out.println("TC: two element names that normalize to the same slug are an error");
        BpmnModelInstance model = model(
                "<process id=\"p\" name=\"P\">"
                        + "<serviceTask id=\"t1\" name=\"Validate Order\" />"
                        + "<serviceTask id=\"t2\" name=\"validate-order\" />"
                        + "</process>");

        List<NameValidation.Issue> issues = NameValidation.validate(model, "p");

        assertTrue(NameValidation.hasErrors(issues));
        assertTrue(hasError(issues, "collision"));
        assertTrue(hasError(issues, "validate_order"));
    }

    @Test
    public void shouldErrorOnDigitLeadingName() {
        System.out.println("TC: a name normalizing to a digit-leading slug is an invalid Java identifier");
        BpmnModelInstance model = model(
                "<process id=\"p\" name=\"P\">"
                        + "<serviceTask id=\"t\" name=\"2 Fast Task\" />"
                        + "</process>");

        List<NameValidation.Issue> issues = NameValidation.validate(model, "p");

        assertTrue(NameValidation.hasErrors(issues));
        assertTrue(hasError(issues, "starts with a digit"));
    }

    @Test
    public void shouldErrorWhenProcessIdHasNoUsableCharacters() {
        System.out.println("TC: an all-non-ASCII process id cannot derive an identifier and is an error");
        BpmnModelInstance model = model(
                "<process id=\"p\" name=\"P\">"
                        + "<serviceTask id=\"t\" name=\"Task\" />"
                        + "</process>");

        List<NameValidation.Issue> issues = NameValidation.validate(model, "日本語");

        assertTrue(NameValidation.hasErrors(issues));
        assertTrue(hasError(issues, "no usable ASCII"));
    }

    @Test
    public void shouldErrorOnReservedWordName() {
        System.out.println("TC: a name normalizing to a Java reserved word is an error");
        BpmnModelInstance model = model(
                "<process id=\"p\" name=\"P\">"
                        + "<serviceTask id=\"t\" name=\"class\" />"
                        + "</process>");

        List<NameValidation.Issue> issues = NameValidation.validate(model, "p");

        assertTrue(NameValidation.hasErrors(issues));
        assertTrue(hasError(issues, "reserved word"));
    }
}
