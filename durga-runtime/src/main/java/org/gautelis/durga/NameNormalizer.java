package org.gautelis.durga;

import java.text.Normalizer;
import java.util.Locale;
import java.util.Set;

/**
 * Shared normalization of BPMN process, activity, and element names into stable
 * ASCII identifiers used for Kafka topic names and generated Java identifiers.
 *
 * <p>Both the scaffolder (which bakes these names into generated code and topic
 * names) and the monitor (which re-derives the same identifiers from a registered
 * BPMN model to match lifecycle events against alarm configurations) rely on this
 * class so the two sides always agree.
 *
 * <p>Non-ASCII letters are transliterated to a reasonable ASCII equivalent rather
 * than dropped: accented Latin letters decompose via Unicode NFD (so Swedish
 * {@code å}/{@code ä} &rarr; {@code a}, {@code ö} &rarr; {@code o}, French
 * {@code é} &rarr; {@code e}, {@code ç} &rarr; {@code c}, and so on), and a small
 * table covers common letters that do not decompose ({@code ø}, {@code æ},
 * {@code ß}, {@code þ}, {@code ð}, {@code ł}, ...). Anything still non-ASCII after
 * that (e.g. CJK) is removed by the final cleanup.
 */
public final class NameNormalizer {

    private NameNormalizer() {
    }

    private static final Set<String> JAVA_RESERVED_WORDS = Set.of(
            "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class",
            "const", "continue", "default", "do", "double", "else", "enum", "extends", "final",
            "finally", "float", "for", "goto", "if", "implements", "import", "instanceof", "int",
            "interface", "long", "native", "new", "package", "private", "protected", "public",
            "return", "short", "static", "strictfp", "super", "switch", "synchronized", "this",
            "throw", "throws", "transient", "try", "void", "volatile", "while",
            "true", "false", "null", "_");

    /**
     * Transliterates accented / special Latin letters to ASCII. Combining diacritics
     * are removed via NFD decomposition; a small table handles letters that do not
     * decompose. Characters that remain non-ASCII are left in place for the caller's
     * downstream cleanup to strip.
     */
    public static String transliterate(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        StringBuilder pre = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case 'Æ' -> pre.append("AE");
                case 'æ' -> pre.append("ae");
                case 'Ø' -> pre.append('O');
                case 'ø' -> pre.append('o');
                case 'Œ' -> pre.append("OE");
                case 'œ' -> pre.append("oe");
                case 'ß' -> pre.append("ss");
                case 'Þ' -> pre.append("TH");
                case 'þ' -> pre.append("th");
                case 'Ð', 'Đ' -> pre.append('D');
                case 'ð', 'đ' -> pre.append('d');
                case 'Ł' -> pre.append('L');
                case 'ł' -> pre.append('l');
                case 'Ħ' -> pre.append('H');
                case 'ħ' -> pre.append('h');
                case 'ı' -> pre.append('i');
                case 'İ' -> pre.append('I');
                case 'ŉ' -> pre.append('n');
                default -> pre.append(c);
            }
        }
        String decomposed = Normalizer.normalize(pre.toString(), Normalizer.Form.NFD);
        return decomposed.replaceAll("\\p{M}+", "");
    }

    /**
     * Produces a lowercase {@code [a-z0-9_]} slug: transliterate, lowercase, collapse
     * runs of other characters to {@code _}, and trim leading/trailing underscores.
     * Returns an empty string when the input is null, blank, or reduces to nothing.
     */
    public static String slug(String value) {
        if (value == null) {
            return "";
        }
        return transliterate(value).toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+", "")
                .replaceAll("_+$", "");
    }

    /** UpperCamelCase form derived from {@link #slug(String)}; empty when the slug is empty. */
    public static String toClassName(String value) {
        String slug = slug(value);
        if (slug.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder(slug.length());
        for (String part : slug.split("_")) {
            if (part.isEmpty()) {
                continue;
            }
            builder.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        }
        return builder.toString();
    }

    /** Whether {@code slug} would be an illegal Java identifier because it starts with a digit. */
    public static boolean startsWithDigit(String slug) {
        return slug != null && !slug.isEmpty() && Character.isDigit(slug.charAt(0));
    }

    /** Whether {@code slug} is a Java reserved word (keyword or reserved literal). */
    public static boolean isReservedJavaWord(String slug) {
        return slug != null && JAVA_RESERVED_WORDS.contains(slug);
    }
}
