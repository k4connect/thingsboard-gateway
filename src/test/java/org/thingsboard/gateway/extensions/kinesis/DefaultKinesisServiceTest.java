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

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import java.io.IOException;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.thingsboard.gateway.extensions.kinesis.Kinesis;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;



@RunWith(MockitoJUnitRunner.class)
public class DefaultKinesisServiceTest {
    private static final String EXTENSION_ID = "kinesis";
    private static final String EXTENSION_TYPE = "KINESIS";

    private static final String CONFIG_FILE_NAME = "kinesis-config.json";
    private static final String INVALID_CONFIG_FILE_NAME = "foobar.json";

    private static final String SERVICE_INIT_ERROR_MESSAGE =
        "ERROR: Exception occurred when trying to initialize DefaultKinesisService.";

    private static final String KINESIS_CONFIG_JSON_STRING =
        "{\"kinesisStreamConfigurations\": [{\"stream\": \"MyEventStream-test\"}]}";

    @Mock
    private GatewayService gateway;

    @Mock
    private Worker mockWorker;

    private JsonNode configurationNode;

    private KinesisStreamConfiguration streamConfig = null;

    private Kinesis extension = null;

    private TbExtensionConfiguration extensionConfig = null;


    @Before
    public void setup() {
        setupExtensionConfiguration();

        setupKinesisExtension();
    }


    private void setupExtensionConfiguration() {
        configurationNode = makeConfigurationNode();
        extensionConfig = makeExtensionConfiguration(configurationNode, CONFIG_FILE_NAME);
    }


    private JsonNode makeConfigurationNode() {
        JsonNode configurationNode = null;

        ObjectMapper jsonMapper = new ObjectMapper();

        try {
            configurationNode = jsonMapper.readTree(KINESIS_CONFIG_JSON_STRING);
        } catch (IOException e) {
            failOnInitException(e);
        }

        return configurationNode;
    }


    private TbExtensionConfiguration makeExtensionConfiguration(JsonNode configurationNode, String configFileName) {
        TbExtensionConfiguration extensionConfig = new TbExtensionConfiguration();

        extensionConfig.setId(EXTENSION_ID);
        extensionConfig.setType(EXTENSION_TYPE);
        extensionConfig.setConfiguration(configurationNode);
        extensionConfig.setExtensionConfiguration(configFileName);

        return extensionConfig;
    }


    private void setupKinesisExtension() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);

        extension = new Kinesis(gateway, streamConfig);
        extension.worker = mockWorker;
    }


    @Test
    public void shouldCreateInstance() {
        DefaultKinesisService service = new DefaultKinesisService(gateway);

        assertNotNull(service);
        assertThat(service, isA(DefaultKinesisService.class));
    }


    @Test
    public void shouldGetNullCurrentConfiguration() {
        DefaultKinesisService service = new DefaultKinesisService(gateway);

        TbExtensionConfiguration currentConfiguration = service.getCurrentConfiguration();
        assertNull(currentConfiguration);
    }


    @Test
    public void shouldInitializeInstanceWithLocalConfiguration() {
        Boolean isRemote = false;

        shouldInitializeInstance(isRemote);
    }


    private void shouldInitializeInstance(Boolean isRemote) {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService(gateway));

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        try {
            serviceSpy.init(extensionConfig, isRemote);

            then(serviceSpy).should().init(extensionConfig, isRemote);
        } catch (Exception e) {
            failOnInitException(e);
        }
    }


    @Test
    public void shouldInitializeInstanceWithRemoteConfiguration() {
        Boolean isRemote = true;

        shouldInitializeInstance(isRemote);
    }


    private void failOnInitException(Exception e) {
        processInitException(e);

        fail(SERVICE_INIT_ERROR_MESSAGE + ";\t" + e);
    }


    private void processInitException(Exception e) {
        verifyInitException(e);
        displayInitException(e);
    }


    private void verifyInitException(Exception e) {
        assertNotNull(e);
        assertNotNull(e.toString());
    }


    private void displayInitException(Exception e) {
        System.out.println(SERVICE_INIT_ERROR_MESSAGE);
        System.out.println(e);
    }


    @Test
    public void shouldThrowExceptionWhenConfigurationNull() {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService(gateway));

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        Boolean isRemote = false;

        try {
            serviceSpy.init(null, isRemote);

            then(serviceSpy).should().init(null, isRemote);
        } catch (Exception e) {
            processInitException(e);
        }
    }


    @Test
    public void shouldThrowExceptionWhenConfigurationInvalid() {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService(gateway));

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        Boolean isRemote = false;

        TbExtensionConfiguration invalidConfigurationToForceException =
            makeExtensionConfiguration(configurationNode, INVALID_CONFIG_FILE_NAME);

        try {
            serviceSpy.init(invalidConfigurationToForceException, isRemote);

            then(serviceSpy).should().init(invalidConfigurationToForceException, isRemote);
        } catch (Exception e) {
            processInitException(e);
        }
    }


    @Test
    public void shouldThrowExceptionWhenInitializeService() {
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService(gateway));

        try {
            serviceSpy.initializeService(null);

            then(serviceSpy).should().initializeService(null);
        } catch (Exception e) {
            processInitException(e);
        }
    }


    @Test
    public void shouldCallDestroyWithNoStreams() {
        // Given
        DefaultKinesisService serviceSpy = spy(new DefaultKinesisService(gateway));

        // When
        serviceSpy.destroy();

        // Then
        then(serviceSpy).should().destroy();
    }


    @Test
    public void shouldCallDestroyWithStreamsForLocalConfig() {
        Boolean isRemote = false;
        shouldCallDestroyWithStreams(isRemote);
    }


    private void shouldCallDestroyWithStreams(Boolean isRemote) {
        DefaultKinesisService serviceSpy =
            spy(new DefaultKinesisService(gateway).currentConfiguration(extensionConfig));

        // Inject a Kinesis object with a mock Worker to avoid performance
        // issues when running unit tests
        when(serviceSpy.buildKinesis(anyObject(), anyObject())).thenReturn(extension);

        try {
            serviceSpy.init(extensionConfig, isRemote);
        } catch (Exception e) {
            failOnInitException(e);
        }

        serviceSpy.destroy();

        then(serviceSpy).should().destroy();
    }

    @Test
    public void shouldCallDestroyWithStreamsForRemoteConfig() {
        Boolean isRemote = true;
        shouldCallDestroyWithStreams(isRemote);
    }
}
