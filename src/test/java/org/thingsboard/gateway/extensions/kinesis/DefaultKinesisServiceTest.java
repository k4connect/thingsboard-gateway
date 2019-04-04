package org.thingsboard.gateway.extensions.kinesis;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.when;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.spy;

import static org.springframework.test.util.ReflectionTestUtils.getField;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.thingsboard.gateway.extensions.kinesis.Kinesis;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;



@RunWith(MockitoJUnitRunner.class)
public class DefaultKinesisServiceTest {
    private static final String CONFIG_FILE_NAME = "kinesis-config.json";

    private static final String CONFIG_FILE_FIELD = "configurationFile";


    @Mock
    private GatewayService gateway;

    @Mock
    private Worker mockWorker;

    private KinesisStreamConfiguration streamConfig = null;

    private Kinesis extension = null;


    @Before
    public void setup() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);

        extension = new Kinesis(gateway, streamConfig);
        extension.worker = mockWorker;
    }


    @Test
    public void shouldCreateInstance() {
        DefaultKinesisService service = new DefaultKinesisService();

        assertNotNull(service);
        assertThat(service, isA(DefaultKinesisService.class));
    }


    @Test
    public void shouldSetConfigFile() {
        // Given
        DefaultKinesisService service = new DefaultKinesisService();

        String configurationFile = getConfigurationFile(service);
        assertNull(configurationFile);

        // When
        setConfigurationFile(service);

        // Then
        configurationFile = getConfigurationFile(service);
        assertNotNull(configurationFile);
        assertEquals(configurationFile, CONFIG_FILE_NAME);
    }


    private String getConfigurationFile(DefaultKinesisService service) {
        return (String) getField(service, CONFIG_FILE_FIELD);
    }


    private void setConfigurationFile(DefaultKinesisService service) {
        // Inject the configuration file name into the service for use in unit
        // testing. This avoids having to use the SpringRunner and @SpringBootTest.
        setField(service, CONFIG_FILE_FIELD, CONFIG_FILE_NAME);
    }


    @Test
    public void shouldInitializeInstance() {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService());
        setConfigurationFile(serviceSpy);

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        try {
            serviceSpy.init();

            then(serviceSpy).should().init();
        } catch (Exception e) {
            failOnInitException(e);
        }
    }


    private void failOnInitException(Exception e) {
        assertNotNull(e);
        assertNotNull(e.toString());

        String errorMessage =
            "ERROR: Exception occurred when trying to initialize DefaultKinesisService.";

        System.out.println(errorMessage);
        System.out.println(e);

        fail(errorMessage + ";\t" + e);
    }


    @Test
    public void shouldCallPreDestroyWithNoStreams() {
        // Given
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService());

        // When
        serviceSpy.preDestroy();

        // Then
        then(serviceSpy).should().preDestroy();
    }


    @Test
    public void shouldCallPreDestroyWithStreams() {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService());
        setConfigurationFile(serviceSpy);

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        try {
            serviceSpy.init();
        } catch (Exception e) {
            failOnInitException(e);
        }

        serviceSpy.preDestroy();

        then(serviceSpy).should().preDestroy();
    }
}
