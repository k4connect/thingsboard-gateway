package org.thingsboard.gateway.extensions.kinesis;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.Mockito.spy;

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;

import com.amazonaws.AmazonClientException;

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

    @Mock
    private GatewayService gateway;

    private KinesisStreamConfiguration streamConfig = null;


    @Before
    public void setup() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);
    }


    @Test
    public void shouldCreateInstance() {
        Kinesis extension = new Kinesis(gateway, streamConfig);

        assertNotNull(extension);
        assertThat(extension, isA(Kinesis.class));
    }


    // TODO March 27, 2019: This test runs very slowly. Need to investigate
    // why or at least mark it with a category of slow tests.
    // TODO April 1, 2019: The peformance issue appears to be caused by calling
    // the init() method.
    @Test
    public void shouldCallInitWithAwsCredentials() {
        Kinesis extensionSpy = spy(new Kinesis(gateway, streamConfig));

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


    // TODO March 27, 2019: This test runs very slowly. Need to investigate
    // why or at least mark it with a category of slow tests.
    // TODO April 1, 2019: The peformance issue appears to be caused by calling
    // the init() method.
    @Test
    public void shouldCallStop() {
        Kinesis extension = new Kinesis(gateway, streamConfig);
        extension.init();

        Kinesis extensionSpy = spy(extension);

        willCallRealMethod().given(extensionSpy).stop();

        extensionSpy.stop();

        then(extensionSpy).should().stop();
    }
}
