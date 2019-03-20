package org.thingsboard.gateway.extensions.kinesis.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import java.util.ArrayList;
import java.util.List;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.junit.Before;
import org.junit.Test;

import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;



public class KinesisConfigurationTest {
    private KinesisConfiguration config = null;


    @Before
    public void setup() {
        config = new KinesisConfiguration();
    }


    @Test
    public void shouldCreateDefaultKinesisConfiguration() {
        assertNotNull(config);
        assertNull(config.kinesisStreamConfigurations);
        assertNull(config.getKinesisStreamConfigurations());
    }


    @Test
    public void shouldCreateKinesisConfigurationWithDefaultStream() {
        List<KinesisStreamConfiguration> streamConfigList = makeStreamConfigList(null);
        config.setKinesisStreamConfigurations(streamConfigList);

        List<KinesisStreamConfiguration> actualStreamConfigList =
            config.getKinesisStreamConfigurations();

        assertEquals(streamConfigList, actualStreamConfigList);
    }


    private List<KinesisStreamConfiguration> makeStreamConfigList(String streamName) {
        KinesisStreamConfiguration streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(streamName);

        List<KinesisStreamConfiguration> streamConfigList =
            new ArrayList<KinesisStreamConfiguration>();
        streamConfigList.add(streamConfig);

        return streamConfigList;
    }


    @Test
    public void shouldSetStreamList() {
        List<KinesisStreamConfiguration> streamConfigList =
            makeStreamConfigList(TEST_STREAM_NAME);
        config.setKinesisStreamConfigurations(streamConfigList);

        List<KinesisStreamConfiguration> actualStreamConfigList =
            config.getKinesisStreamConfigurations();

        assertEquals(streamConfigList, actualStreamConfigList);
    }


    @Test
    public void shouldGenerateStringWithNullStreamList() {
        String expectedStreamListRepresentation = "null";

        String expectedConfigStringRepresentation =
            getExpectedConfigString(expectedStreamListRepresentation);

        assertEquals(expectedConfigStringRepresentation, config.toString());
    }


    private String getExpectedConfigString(String expectedStreamListRepresentation) {
        return "KinesisConfiguration(kinesisStreamConfigurations=" +
                expectedStreamListRepresentation + ")";
    }


    @Test
    public void shouldGenerateStringRepresentation() {
        List<KinesisStreamConfiguration> streamConfigList =
            makeStreamConfigList(TEST_STREAM_NAME);
        config.setKinesisStreamConfigurations(streamConfigList);

        String expectedConfigStringRepresentation =
            getExpectedConfigString(streamConfigList.toString());

        assertEquals(expectedConfigStringRepresentation, config.toString());
    }


    @Test
    public void expectEqualsContract() {

        // Suppress STRICT_INHERITANCE and NONFINAL_FIELDS because the
        // KinesisConfiguration class uses the Lombok @Data annotation
        // and the equals() and hashCode() methods are automatically generated

        EqualsVerifier.forClass(KinesisConfiguration.class)
            .suppress(Warning.STRICT_INHERITANCE)
            .suppress(Warning.NONFINAL_FIELDS)
            .verify();
    }
}
