package org.gautelis.durga.validation;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonComparisonTest {

    @Test
    public void shouldReportEqualForIdenticalPayloads() {
        System.out.println("TC: identical JSON payloads compare equal with no diffs");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"a\":1,\"b\":{\"c\":\"x\"}}",
                "{\"a\":1,\"b\":{\"c\":\"x\"}}",
                List.of());
        assertTrue(report.equal());
        assertTrue(report.diffs().isEmpty());
    }

    @Test
    public void shouldReportValueChange() {
        System.out.println("TC: changed scalar value is reported as VALUE_CHANGED at its path");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"a\":1,\"b\":{\"c\":\"x\"}}",
                "{\"a\":1,\"b\":{\"c\":\"y\"}}",
                List.of());
        assertFalse(report.equal());
        assertEquals(1, report.diffs().size());
        JsonComparison.Diff diff = report.diffs().get(0);
        assertEquals("b.c", diff.path());
        assertEquals(JsonComparison.DiffKind.VALUE_CHANGED, diff.kind());
        assertEquals("\"x\"", diff.priorValue());
        assertEquals("\"y\"", diff.candidateValue());
    }

    @Test
    public void shouldReportAddedAndRemovedFields() {
        System.out.println("TC: fields present on only one side are reported as ADDED/REMOVED");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"kept\":1,\"gone\":2}",
                "{\"kept\":1,\"new\":3}",
                List.of());
        assertFalse(report.equal());
        assertEquals(2, report.diffs().size());
        Map<String, JsonComparison.DiffKind> byPath = report.diffs().stream()
                .collect(java.util.stream.Collectors.toMap(JsonComparison.Diff::path, JsonComparison.Diff::kind));
        assertEquals(JsonComparison.DiffKind.REMOVED, byPath.get("gone"));
        assertEquals(JsonComparison.DiffKind.ADDED, byPath.get("new"));
    }

    @Test
    public void shouldReportTypeChange() {
        System.out.println("TC: differing JSON node types are reported as TYPE_CHANGED");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"a\":\"1\"}",
                "{\"a\":1}",
                List.of());
        assertFalse(report.equal());
        assertEquals(JsonComparison.DiffKind.TYPE_CHANGED, report.diffs().get(0).kind());
    }

    @Test
    public void shouldCompareArraysElementWise() {
        System.out.println("TC: arrays are compared element-wise with indexed paths");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"xs\":[1,2,3]}",
                "{\"xs\":[1,9]}",
                List.of());
        assertFalse(report.equal());
        Map<String, JsonComparison.DiffKind> byPath = report.diffs().stream()
                .collect(java.util.stream.Collectors.toMap(JsonComparison.Diff::path, JsonComparison.Diff::kind));
        assertEquals(JsonComparison.DiffKind.VALUE_CHANGED, byPath.get("xs[1]"));
        assertEquals(JsonComparison.DiffKind.REMOVED, byPath.get("xs[2]"));
    }

    @Test
    public void shouldIgnoreExactPathAndSubtree() {
        System.out.println("TC: an ignore path suppresses that node and everything beneath it");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"data\":1,\"meta\":{\"ts\":\"a\",\"nested\":{\"x\":1}}}",
                "{\"data\":1,\"meta\":{\"ts\":\"b\",\"nested\":{\"x\":2}}}",
                List.of("meta"));
        assertTrue(report.equal());
    }

    @Test
    public void shouldIgnoreArrayFieldViaWildcard() {
        System.out.println("TC: items[*].timestamp ignores that field across all array elements");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"items\":[{\"id\":1,\"timestamp\":\"t1\"},{\"id\":2,\"timestamp\":\"t2\"}]}",
                "{\"items\":[{\"id\":1,\"timestamp\":\"x1\"},{\"id\":2,\"timestamp\":\"x2\"}]}",
                List.of("items[*].timestamp"));
        assertTrue(report.equal());
    }

    @Test
    public void shouldStillReportNonIgnoredDifferenceWithinIgnoredSibling() {
        System.out.println("TC: wildcard ignore of one field still surfaces other real differences");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"items\":[{\"id\":1,\"timestamp\":\"t1\"}]}",
                "{\"items\":[{\"id\":2,\"timestamp\":\"x1\"}]}",
                List.of("items[*].timestamp"));
        assertFalse(report.equal());
        assertEquals(1, report.diffs().size());
        assertEquals("items[0].id", report.diffs().get(0).path());
    }

    @Test
    public void shouldIgnoreFieldWildcardSegment() {
        System.out.println("TC: a leading * wildcard matches any object field segment");
        JsonComparison.Report report = JsonComparison.compare(
                "{\"east\":{\"ts\":1},\"west\":{\"ts\":2}}",
                "{\"east\":{\"ts\":9},\"west\":{\"ts\":8}}",
                List.of("*.ts"));
        assertTrue(report.equal());
    }

    @Test
    public void shouldTokenizePathsWithIndicesAndFields() {
        System.out.println("TC: path tokenizer splits fields and [n] indices");
        assertEquals(List.of("items", "[*]", "ts"), JsonComparison.parsePathForTest("items[*].ts"));
        assertEquals(List.of("a", "b", "[0]", "c"), JsonComparison.parsePathForTest("a.b[0].c"));
    }
}
