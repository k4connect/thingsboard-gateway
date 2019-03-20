package org.thingsboard.gateway.extensions.kinesis.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.junit.Before;
import org.junit.Test;

import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;



public class KinesisStreamConfigurationTest {
    public static final String TEST_STREAM_NAME = "test-stream";

    private KinesisStreamConfiguration config = null;


    @Before
    public void setup() {
        config = new KinesisStreamConfiguration();
    }


    @Test
    public void shouldCreateKinesisStreamConfiguration() {
        assertNotNull(config);
        assertNull(config.stream);
        assertNull(config.getStream());
    }


    @Test
    public void shouldSetStream() {
        assertNull(config.getStream());

        config.setStream(TEST_STREAM_NAME);

        assertNotNull(config.getStream());
    }


    @Test
    public void shouldGenerateStringWithNullStream() {
        String expectedStreamRepresentation = "null";

        String expectedConfigStringRepresentation =
            getExpectedConfigString(expectedStreamRepresentation);

        assertEquals(expectedConfigStringRepresentation, config.toString());
    }


    private String getExpectedConfigString(String expectedStreamName) {
        return "KinesisStreamConfiguration(stream=" + expectedStreamName + ")";
    }


    @Test
    public void shouldGenerateStringRepresentation() {
        config.setStream(TEST_STREAM_NAME);

        String expectedStreamRepresentation = TEST_STREAM_NAME;
        String expectedConfigStringRepresentation =
            getExpectedConfigString(expectedStreamRepresentation);

        assertEquals(expectedConfigStringRepresentation, config.toString());
    }


    @Test
    public void expectEqualsContract() {

        // Suppress STRICT_INHERITANCE and NONFINAL_FIELDS because the
        // KinesisStreamConfiguration class uses the Lombok @Data annotation
        // and the equals() and hashCode() methods are automatically generated

        EqualsVerifier.forClass(KinesisStreamConfiguration.class)
            .suppress(Warning.STRICT_INHERITANCE)
            .suppress(Warning.NONFINAL_FIELDS)
            .verify();
    }
}
