package org.thingsboard.gateway.extensions.kinesis;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.spy;

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;

import java.lang.NullPointerException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.thingsboard.gateway.extensions.kinesis.AmazonKinesisApplicationRecordProcessor;
import org.thingsboard.gateway.extensions.kinesis.Kinesis;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;



@RunWith(MockitoJUnitRunner.class)
public class AmazonKinesisApplicationRecordProcessorTest {
    @Mock
    GatewayService gateway;

    @Mock
    InitializationInput initializationInput;

    @Mock
    ShutdownInput shutdownInput;

    @Mock
    IRecordProcessorCheckpointer checkpointer;

    @Mock
    ProcessRecordsInput processRecordsInput;

    private AmazonKinesisApplicationRecordProcessor processorSpy;

    private KinesisStreamConfiguration streamConfig = null;

    private Kinesis extension = null;


    @Before
    public void setup() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);

        extension = new Kinesis(gateway, streamConfig);

        processorSpy = spy(new AmazonKinesisApplicationRecordProcessor(extension));

        willCallRealMethod().given(processorSpy).initialize(initializationInput);
    }


    @Test
    public void shouldCreateInstance() {
        AmazonKinesisApplicationRecordProcessor processor =
            new AmazonKinesisApplicationRecordProcessor(extension);

        assertNotNull(processor);
    }


    @Test
    public void shouldThrowNullPointerExceptionWhenInitializeInstance() {
        AmazonKinesisApplicationRecordProcessor processor =
            new AmazonKinesisApplicationRecordProcessor(extension);

        try {
            processor.initialize(null);
        } catch (NullPointerException e) {
            assertNotNull(e);
        }
    }


    @Test
    public void shouldInitializeInstance() {
        processorSpy.initialize(initializationInput);

        then(processorSpy).should().initialize(initializationInput);
    }


    @Test
    public void shouldShutdownInstance() {
        processorSpy.shutdown(shutdownInput);

        then(processorSpy).should().shutdown(shutdownInput);
    }


    @Test
    public void shouldShutdownInstanceWithShutdownReason() {
        setShutdownStubs();

        processorSpy.shutdown(shutdownInput);

        then(processorSpy).should().shutdown(shutdownInput);
    }


    private void setShutdownStubs() {
        given(shutdownInput.getShutdownReason()).willReturn(ShutdownReason.TERMINATE);
        given(shutdownInput.getCheckpointer()).willReturn(checkpointer);
    }


    @Test
    public void expectShutdownException() throws InvalidStateException, ShutdownException {
        setShutdownStubs();
        willThrow(ShutdownException.class).given(checkpointer).checkpoint();

        processorSpy.shutdown(shutdownInput);

        then(processorSpy).should().shutdown(shutdownInput);
    }


    @Test
    public void expectInvalidStateException() throws InvalidStateException, ShutdownException {
        setShutdownStubs();
        willThrow(InvalidStateException.class).given(checkpointer).checkpoint();

        processorSpy.shutdown(shutdownInput);

        then(processorSpy).should().shutdown(shutdownInput);
    }


    @Test
    public void expectThrottlingException() throws InvalidStateException, ShutdownException {
        setShutdownStubs();
        willThrow(ThrottlingException.class).given(checkpointer).checkpoint();

        processorSpy.shutdown(shutdownInput);

        then(processorSpy).should().shutdown(shutdownInput);
    }


    @Test
    public void shouldProcessRecordsWithValidData()
            throws CharacterCodingException {

        String someTestData = "foobar";
        List<Record> records = makeRecordList(someTestData, UTF_8);
        setupProcessRecordsInputStubs(records);

        processorSpy.processRecords(processRecordsInput);

        then(processorSpy).should().processRecords(processRecordsInput);
    }


    private List<Record> makeRecordList(String someTestData, Charset charset)
            throws CharacterCodingException {

        CharsetEncoder encoder = Charset.forName(charset.name()).newEncoder();
        CharBuffer charBuffer = CharBuffer.wrap(someTestData.toCharArray());
        ByteBuffer buffer = encoder.encode(charBuffer);

        Record dataRecord = new Record().withData(buffer);
        List<Record> records = new ArrayList();
        records.add(dataRecord);

        return records;
    }


    private void setupProcessRecordsInputStubs(List<Record> records) {
        given(processRecordsInput.getRecords()).willReturn(records);
        given(processRecordsInput.getCheckpointer()).willReturn(checkpointer);
        willCallRealMethod().given(processorSpy).processRecords(processRecordsInput);
    }


    @Test
    public void expectCharacterCodingExceptionWhenProcessRecords()
            throws CharacterCodingException {

        String someTestData = "Olé";

        // Use ISO_8859_1 to cause a CharacterCodingException to occur in
        // AmazonKinesisApplicationRecordProcessor.processRecords();
        Charset charsetToCauseException = ISO_8859_1;

        List<Record> records = makeRecordList(someTestData, charsetToCauseException);
        setupProcessRecordsInputStubs(records);

        processorSpy.processRecords(processRecordsInput);

        then(processorSpy).should().processRecords(processRecordsInput);
    }
}
