package org.gautelis.durga;

import org.junit.Test;

import static org.junit.Assert.*;

public class NameNormalizerTest {

    @Test
    public void shouldTransliterateSwedishCharacters() {
        System.out.println("TC: Swedish a-ring/a-diaeresis map to a, o-diaeresis maps to o");
        assertEquals("aterkop", NameNormalizer.slug("Återköp"));
        assertEquals("overforing", NameNormalizer.slug("Överföring"));
        assertEquals("arende", NameNormalizer.slug("Ärende"));
        assertEquals("aao", NameNormalizer.slug("åäö"));
    }

    @Test
    public void shouldTransliterateOtherEuropeanCharacters() {
        System.out.println("TC: transliterates common accented and special Latin letters to ASCII");
        assertEquals("cafe", NameNormalizer.slug("café"));
        assertEquals("munchen", NameNormalizer.slug("München"));
        assertEquals("naive", NameNormalizer.slug("naïve"));
        assertEquals("strasse", NameNormalizer.slug("Straße"));
        assertEquals("bjorod", NameNormalizer.slug("Bjørød"));
        assertEquals("aeble", NameNormalizer.slug("æble"));
    }

    @Test
    public void shouldProduceLowercaseSlugWithUnderscores() {
        System.out.println("TC: collapses punctuation/whitespace to single underscores and trims ends");
        assertEquals("kop_salj", NameNormalizer.slug("Köp & Sälj"));
        assertEquals("order_fulfillment", NameNormalizer.slug("order-fulfillment"));
        assertEquals("a_b_c", NameNormalizer.slug("  a...b   c  "));
    }

    @Test
    public void shouldReturnEmptyForNonDerivableInput() {
        System.out.println("TC: slug is empty for null, blank, or all-non-transliterable input");
        assertEquals("", NameNormalizer.slug(null));
        assertEquals("", NameNormalizer.slug("   "));
        assertEquals("", NameNormalizer.slug("日本語"));
    }

    @Test
    public void shouldBuildUpperCamelClassNames() {
        System.out.println("TC: toClassName produces UpperCamelCase from a transliterated slug");
        assertEquals("Aterkop", NameNormalizer.toClassName("Återköp"));
        assertEquals("RegistreraArende", NameNormalizer.toClassName("Registrera ärende"));
        assertEquals("", NameNormalizer.toClassName("日本語"));
    }

    @Test
    public void shouldDetectDigitLeadingAndReservedWords() {
        System.out.println("TC: flags digit-leading slugs and Java reserved words");
        assertTrue(NameNormalizer.startsWithDigit(NameNormalizer.slug("2 Fast Process")));
        assertFalse(NameNormalizer.startsWithDigit(NameNormalizer.slug("fast")));
        assertTrue(NameNormalizer.isReservedJavaWord("class"));
        assertTrue(NameNormalizer.isReservedJavaWord("default"));
        assertTrue(NameNormalizer.isReservedJavaWord("null"));
        assertFalse(NameNormalizer.isReservedJavaWord("validate"));
    }
}
