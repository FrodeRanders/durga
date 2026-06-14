package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class JsonSchemaValidatorTest {

    private final JsonSchemaValidator validator = new JsonSchemaValidator();

    @Test
    public void shouldPassValidPayload() throws Exception {
        System.out.println("TC: returns payload unchanged when valid against schema");
        String config = "{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"string\"}}}";
        byte[] result = validator.execute(Plugin.toBytes("{\"name\":\"Alice\"}"), config);
        assertEquals("{\"name\":\"Alice\"}", Plugin.toString(result));
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMissingRequired() throws Exception {
        System.out.println("TC: throws ValidationException when required field is missing");
        String config = "{\"type\":\"object\",\"required\":[\"name\"]}";
        validator.execute(Plugin.toBytes("{\"email\":\"a@b.com\"}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailWrongType() throws Exception {
        System.out.println("TC: throws ValidationException when field type does not match schema");
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\"}}}";
        validator.execute(Plugin.toBytes("{\"age\":\"not-a-number\"}"), config);
    }

    @Test
    public void shouldAcceptIntegerAsNumber() throws Exception {
        System.out.println("TC: accepts integer value matching integer schema type");
        String config = "{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"integer\"}}}";
        validator.execute(Plugin.toBytes("{\"value\":42}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailBelowMinimum() throws Exception {
        System.out.println("TC: throws ValidationException when integer value is below minimum");
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"minimum\":18}}}";
        validator.execute(Plugin.toBytes("{\"age\":16}"), config);
    }

    @Test
    public void shouldPassMinimumBoundary() throws Exception {
        System.out.println("TC: accepts integer value equal to minimum boundary");
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"minimum\":18}}}";
        validator.execute(Plugin.toBytes("{\"age\":18}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailAboveMaximum() throws Exception {
        System.out.println("TC: throws ValidationException when integer value exceeds maximum");
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"maximum\":120}}}";
        validator.execute(Plugin.toBytes("{\"age\":150}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailEnumMismatch() throws Exception {
        System.out.println("TC: throws ValidationException when string value is not in enum");
        String config = "{\"type\":\"object\",\"properties\":{\"tier\":{\"enum\":[\"gold\",\"silver\",\"bronze\"]}}}";
        validator.execute(Plugin.toBytes("{\"tier\":\"platinum\"}"), config);
    }

    @Test
    public void shouldPassEnumMatch() throws Exception {
        System.out.println("TC: accepts string value that matches an enum entry");
        String config = "{\"type\":\"object\",\"properties\":{\"tier\":{\"enum\":[\"gold\",\"silver\",\"bronze\"]}}}";
        validator.execute(Plugin.toBytes("{\"tier\":\"gold\"}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMinLength() throws Exception {
        System.out.println("TC: throws ValidationException when string length is below minLength");
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"minLength\":3}}}";
        validator.execute(Plugin.toBytes("{\"code\":\"ab\"}"), config);
    }

    @Test
    public void shouldPassMinLengthBoundary() throws Exception {
        System.out.println("TC: accepts string equal to minLength boundary");
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"minLength\":3}}}";
        validator.execute(Plugin.toBytes("{\"code\":\"abc\"}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMaxLength() throws Exception {
        System.out.println("TC: throws ValidationException when string length exceeds maxLength");
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"maxLength\":5}}}";
        validator.execute(Plugin.toBytes("{\"code\":\"abcdef\"}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailRegexPattern() throws Exception {
        System.out.println("TC: throws ValidationException when string does not match regex pattern");
        String config = "{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\",\"pattern\":\".+@.+\\\\..+\"}}}";
        validator.execute(Plugin.toBytes("{\"email\":\"invalid\"}"), config);
    }

    @Test
    public void shouldPassRegexPattern() throws Exception {
        System.out.println("TC: accepts string matching regex pattern for email format");
        String config = "{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\",\"pattern\":\".+@.+\\\\..+\"}}}";
        validator.execute(Plugin.toBytes("{\"email\":\"a@b.com\"}"), config);
    }

    @Test
    public void shouldValidateNestedObject() throws Exception {
        System.out.println("TC: validates nested object with required fields against schema");
        String config = "{\"type\":\"object\",\"required\":[\"data\"],\"properties\":{\"data\":{\"type\":\"object\",\"required\":[\"x\",\"y\"],\"properties\":{\"x\":{\"type\":\"integer\"},\"y\":{\"type\":\"integer\"}}}}}";
        validator.execute(Plugin.toBytes("{\"data\":{\"x\":1,\"y\":2}}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailNestedRequired() throws Exception {
        System.out.println("TC: throws ValidationException when nested object is missing required field");
        String config = "{\"type\":\"object\",\"required\":[\"data\"],\"properties\":{\"data\":{\"type\":\"object\",\"required\":[\"x\",\"y\"],\"properties\":{\"x\":{\"type\":\"integer\"},\"y\":{\"type\":\"integer\"}}}}}";
        validator.execute(Plugin.toBytes("{\"data\":{\"x\":1}}"), config);
    }

    @Test
    public void shouldValidateArrayItems() throws Exception {
        System.out.println("TC: validates array items conform to item type schema");
        String config = "{\"type\":\"object\",\"properties\":{\"items\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}}";
        validator.execute(Plugin.toBytes("{\"items\":[1,2,3]}"), config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailArrayItemType() throws Exception {
        System.out.println("TC: throws ValidationException when array item type does not match schema");
        String config = "{\"type\":\"object\",\"properties\":{\"items\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}}";
        validator.execute(Plugin.toBytes("{\"items\":[1,\"bad\",3]}"), config);
    }
}
