package org.gautelis.durga.plugins;

import org.junit.Test;

import static org.junit.Assert.*;

public class JsonSchemaValidatorTest {

    private final JsonSchemaValidator validator = new JsonSchemaValidator();

    @Test
    public void shouldPassValidPayload() throws Exception {
        String config = "{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"string\"}}}";
        String result = validator.execute("{\"name\":\"Alice\"}", config);
        assertEquals("{\"name\":\"Alice\"}", result);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMissingRequired() throws Exception {
        String config = "{\"type\":\"object\",\"required\":[\"name\"]}";
        validator.execute("{\"email\":\"a@b.com\"}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailWrongType() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\"}}}";
        validator.execute("{\"age\":\"not-a-number\"}", config);
    }

    @Test
    public void shouldAcceptIntegerAsNumber() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"integer\"}}}";
        validator.execute("{\"value\":42}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailBelowMinimum() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"minimum\":18}}}";
        validator.execute("{\"age\":16}", config);
    }

    @Test
    public void shouldPassMinimumBoundary() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"minimum\":18}}}";
        validator.execute("{\"age\":18}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailAboveMaximum() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"age\":{\"type\":\"integer\",\"maximum\":120}}}";
        validator.execute("{\"age\":150}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailEnumMismatch() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"tier\":{\"enum\":[\"gold\",\"silver\",\"bronze\"]}}}";
        validator.execute("{\"tier\":\"platinum\"}", config);
    }

    @Test
    public void shouldPassEnumMatch() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"tier\":{\"enum\":[\"gold\",\"silver\",\"bronze\"]}}}";
        validator.execute("{\"tier\":\"gold\"}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMinLength() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"minLength\":3}}}";
        validator.execute("{\"code\":\"ab\"}", config);
    }

    @Test
    public void shouldPassMinLengthBoundary() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"minLength\":3}}}";
        validator.execute("{\"code\":\"abc\"}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailMaxLength() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"code\":{\"type\":\"string\",\"maxLength\":5}}}";
        validator.execute("{\"code\":\"abcdef\"}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailRegexPattern() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\",\"pattern\":\".+@.+\\\\..+\"}}}";
        validator.execute("{\"email\":\"invalid\"}", config);
    }

    @Test
    public void shouldPassRegexPattern() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\",\"pattern\":\".+@.+\\\\..+\"}}}";
        validator.execute("{\"email\":\"a@b.com\"}", config);
    }

    @Test
    public void shouldValidateNestedObject() throws Exception {
        String config = "{\"type\":\"object\",\"required\":[\"data\"],\"properties\":{\"data\":{\"type\":\"object\",\"required\":[\"x\",\"y\"],\"properties\":{\"x\":{\"type\":\"integer\"},\"y\":{\"type\":\"integer\"}}}}}";
        validator.execute("{\"data\":{\"x\":1,\"y\":2}}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailNestedRequired() throws Exception {
        String config = "{\"type\":\"object\",\"required\":[\"data\"],\"properties\":{\"data\":{\"type\":\"object\",\"required\":[\"x\",\"y\"],\"properties\":{\"x\":{\"type\":\"integer\"},\"y\":{\"type\":\"integer\"}}}}}";
        validator.execute("{\"data\":{\"x\":1}}", config);
    }

    @Test
    public void shouldValidateArrayItems() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"items\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}}";
        validator.execute("{\"items\":[1,2,3]}", config);
    }

    @Test(expected = JsonSchemaValidator.ValidationException.class)
    public void shouldFailArrayItemType() throws Exception {
        String config = "{\"type\":\"object\",\"properties\":{\"items\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}}}";
        validator.execute("{\"items\":[1,\"bad\",3]}", config);
    }
}
