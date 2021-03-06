package org.thingsboard.gateway.extensions.kinesis;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.Mockito.spy;

import static org.thingsboard.gateway.extensions.kinesis.Kinesis.EVENTS_STARTED_PATH;
import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;

import com.amazonaws.AmazonClientException;
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
public class KinesisTest {
    public static final String AWS_CONFIG_DIR_NAME = ".aws";
    public static final String AWS_CONFIG_DIR_PATH_NAME =
        System.getProperty("user.home") + "/" + AWS_CONFIG_DIR_NAME + "/";

    public static final String CREDENTIALS_FILE_NAME = "credentials";
    public static final String CREDENTIALS_FILE_PATH_NAME =
        AWS_CONFIG_DIR_PATH_NAME + CREDENTIALS_FILE_NAME;

    public static final String NEW_CREDENTIALS_FILE_NAME = CREDENTIALS_FILE_NAME + ".ignore";
    public static final String NEW_CREDENTIALS_FILE_PATH_NAME =
        AWS_CONFIG_DIR_PATH_NAME + NEW_CREDENTIALS_FILE_NAME;

    private static final Path CREDENTIALS_FILE_PATH = Paths.get(CREDENTIALS_FILE_PATH_NAME);
    private static final Path NEW_CREDENTIALS_FILE_PATH = Paths.get(NEW_CREDENTIALS_FILE_PATH_NAME);

    private static final String PATH_KEY = "path";

    private static final String DEVICES_PATH = "Devices";
    private static final String VARIABLES_PATH = "variables";
    private static final String BRIDGES_PATH = makePath(DEVICES_PATH, "Bridges");
    private static final String SERVERS_PATH = makePath(DEVICES_PATH, "Servers");
    private static final String EVENTS_PATH = "events";

    private static final String BRIDGES_EVENTS_STARTED_PATH = BRIDGES_PATH + EVENTS_STARTED_PATH;
    private static final String SERVERS_EVENTS_STARTED_PATH = SERVERS_PATH + EVENTS_STARTED_PATH;

    private static final String VARIABLES_MESSAGE_PATH = makePath(DEVICES_PATH, VARIABLES_PATH);
    private static final String EVENTS_MESSAGE_PATH = makePath(DEVICES_PATH, EVENTS_PATH);

    private static final String EXPECTED_OID = "1.3.6.1.4.1.32473.1.2";
    private static final String EXPECTED_TYPE = "disconnect";


    @Mock
    private GatewayService gateway;

    @Mock
    private Worker worker;

    private KinesisStreamConfiguration streamConfig = null;
    private Kinesis extension = null;



    private static String makePath(String source, String suffix) {
        return source + "/" + suffix;
    }


    @Before
    public void setup() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);

        extension = new Kinesis(gateway, streamConfig);
    }


    @Test
    public void shouldCreateInstance() {
        Kinesis extension = new Kinesis(gateway, streamConfig);

        assertNotNull(extension);
        assertThat(extension, isA(Kinesis.class));
    }


    // TODO April 2, 2019: This test runs very slowly. The performance issue
    // appears to be caused by creating a real instance of a
    // com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker in the
    // init() method.
    //
    // Eventually, this test should be added to a "slow" category and run
    // less frequently. However, for now, inject a mock Worker into the instance
    // of the extension to reduce test execution time for this test from ~204
    // seconds to a few milliseconds.
    @Test
    public void shouldCallInitWithAwsCredentials() {
        // Inject a mock worker to reduce execution time
        extension.worker = worker;

        Kinesis extensionSpy = spy(extension);

        willCallRealMethod().given(extensionSpy).init();

        extensionSpy.init();

        then(extensionSpy).should().init();
    }


    @Test
    public void shouldCallInitWithoutAwsCredentials() {
        Kinesis extensionSpy = spy(new Kinesis(gateway, streamConfig));

        willCallRealMethod().given(extensionSpy).init();

        // Temporarily rename the AWS credentials file to "hide" it from the
        // Kinesis.init() method to cause an exception to occur.
        renameCredentialsFile(CREDENTIALS_FILE_PATH, NEW_CREDENTIALS_FILE_NAME);

        try {
            extensionSpy.init();
        } catch (Exception e) {
            assertNotNull(e);
            assertThat(e, instanceOf(AmazonClientException.class));
            assertNotNull(e.toString());
        } finally {
            // Restore the name of the AWS credentials file to its original
            // value so that other test cases can run correctly.
            renameCredentialsFile(NEW_CREDENTIALS_FILE_PATH, CREDENTIALS_FILE_NAME);
        }
    }


    private void renameCredentialsFile(Path originalFilePath, String newFileName) {
        try {
            Files.move(originalFilePath, originalFilePath.resolveSibling(newFileName));
        } catch (IOException e) {
            String errorMessage =
                "ERROR: IOException occurred when trying to rename file " +
                    "from: " + originalFilePath + " to: " + newFileName;

            System.out.println(errorMessage);
            System.out.println(e);

            fail(errorMessage + ";\t" + e);
        }
    }


    @Test
    public void shouldCallStop() {
        extension.worker = worker;

        Kinesis extensionSpy = spy(extension);

        willCallRealMethod().given(extensionSpy).stop();

        extensionSpy.stop();

        then(extensionSpy).should().stop();
    }


    @Test
    public void shouldCallProcessBodyWithNullBody() {
        testProcessBody(null);
    }


    private void testProcessBody(String body) {
        Kinesis extension = new Kinesis(gateway, streamConfig);
        extension.worker = worker;

        // Call the "real" init() method, but use the mock worker injected above.
        extension.init();

        Kinesis extensionSpy = spy(extension);

        willCallRealMethod().given(extensionSpy).processBody(body);

        extensionSpy.processBody(body);

        then(extensionSpy).should().processBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithEmptyBody() {
        testProcessBody("");
    }


    @Test
    public void shouldCallProcessBodyWithEmptyObjectBody() {
        testProcessBody("{}");
    }


    @Test
    public void shouldCallProcessBodyWithVariablesEventsEmptyPath() {
        String body = makeBody("");

        testProcessBody(body);
    }


    private String makeBody(String pathValue) {
        return "{ \"" + PATH_KEY + "\": \"" + pathValue + "\" }";
    }


    @Test
    public void shouldCallProcessBodyWithVariablesEventsNonEmptyPath() {
        String body = makeBody("foobar");

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariablesEventsDevicesPath() {
        String body = makeBody(DEVICES_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariablesEventsDevicesPathWithVariables() {
        String body = makeBody(VARIABLES_MESSAGE_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariableEventsDevicesPathWithEvents() {
        String body = makeBody(EVENTS_MESSAGE_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariableEventsDevicesPathWithBridgesEvents() {
        String body = makeBody(makePath(BRIDGES_PATH, EVENTS_PATH));

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariableEventsDevicesPathWithBridgesNoEvents() {
        String body = makeBody(BRIDGES_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariableEventsDevicesPathWithServersEvents() {
        String body = makeBody(makePath(SERVERS_PATH, EVENTS_PATH));

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithVariableEventsDevicesPathWithServersNoEvents() {
        String body = makeBody(SERVERS_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithStartedEventInvalidDevice() {
        String body = makeBody(makePath(EVENTS_MESSAGE_PATH, "started/foobar"));

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithBridgesEventStarted() {
        String body = makeBody(BRIDGES_EVENTS_STARTED_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithServersEventStarted() {
        String body = makeBody(SERVERS_EVENTS_STARTED_PATH);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithServersDisconnectNullOidNullType() {
        String body = makeDisconnectBody(null, null);

        testProcessBody(body);
    }


    private String makeDisconnectBody(String oid, String type) {
        return  "{ \"oid\": \"" + oid + "\", \"type\": \"" + type + "\"  }";
    }


    @Test
    public void shouldCallProcessBodyWithServersDisconnectExpectedOidNullType() {
        String body = makeDisconnectBody(EXPECTED_OID, null);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithServersDisconnectExpectedOidExpectedType() {
        String body = makeDisconnectBody(EXPECTED_OID, EXPECTED_TYPE);

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithServersDisconnectExpectedOidInvalidType() {
        String body = makeDisconnectBody(EXPECTED_OID, "foobar");

        testProcessBody(body);
    }


    @Test
    public void shouldCallProcessBodyWithServersDisconnectInvalidOidNullType() {
        String body = makeDisconnectBody("foobar", null);

        testProcessBody(body);
    }


}
