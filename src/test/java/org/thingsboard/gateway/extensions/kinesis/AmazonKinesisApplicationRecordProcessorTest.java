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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.thingsboard.gateway.extensions.kinesis.AmazonKinesisApplicationRecordProcessor;
import org.thingsboard.gateway.extensions.kinesis.Kinesis;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;


import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;



@RunWith(MockitoJUnitRunner.class)
public class AmazonKinesisApplicationRecordProcessorTest {
    @Mock
    GatewayService gateway;

    @Mock
    InitializationInput initializationInput;

    @Mock
    ShutdownRequestedInput shutdownRequestedInput;

    @Mock
    ShardEndedInput shardEndedInput;

    @Mock
    RecordProcessorCheckpointer checkpointer;

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
    public void shouldCallShutdownRequested() {
        setShutdownStubs();

        processorSpy.shutdownRequested(shutdownRequestedInput);

        then(processorSpy).should().shutdownRequested(shutdownRequestedInput);
    }


    private void setShutdownStubs() {
        given(shutdownRequestedInput.checkpointer()).willReturn(checkpointer);

        given(shardEndedInput.checkpointer()).willReturn(checkpointer);
    }


    @Test
    public void expectShutdownExceptionWhenShutdownRequested()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(ShutdownException.class).given(checkpointer).checkpoint();

        processorSpy.shutdownRequested(shutdownRequestedInput);

        then(processorSpy).should().shutdownRequested(shutdownRequestedInput);
    }


    @Test
    public void expectInvalidStateExceptionWhenShutdownRequested()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(InvalidStateException.class).given(checkpointer).checkpoint();

        processorSpy.shutdownRequested(shutdownRequestedInput);

        then(processorSpy).should().shutdownRequested(shutdownRequestedInput);
    }


    @Test
    public void expectThrottlingExceptionWhenShutdownRequested()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(ThrottlingException.class).given(checkpointer).checkpoint();

        processorSpy.shutdownRequested(shutdownRequestedInput);

        then(processorSpy).should().shutdownRequested(shutdownRequestedInput);
    }


    @Test
    public void shouldCallShardEnded() {
        setShutdownStubs();

        processorSpy.shardEnded(shardEndedInput);

        then(processorSpy).should().shardEnded(shardEndedInput);
    }


    @Test
    public void expectShutdownExceptionWhenShardEnded()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(ShutdownException.class).given(checkpointer).checkpoint();

        processorSpy.shardEnded(shardEndedInput);

        then(processorSpy).should().shardEnded(shardEndedInput);
    }


    @Test
    public void expectInvalidStateExceptionWhenShardEnded()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(InvalidStateException.class).given(checkpointer).checkpoint();

        processorSpy.shardEnded(shardEndedInput);

        then(processorSpy).should().shardEnded(shardEndedInput);
    }


    @Test
    public void expectThrottlingExceptionWhenShardEnded()
        throws InvalidStateException, ShutdownException {

        setShutdownStubs();
        willThrow(ThrottlingException.class).given(checkpointer).checkpoint();

        processorSpy.shardEnded(shardEndedInput);

        then(processorSpy).should().shardEnded(shardEndedInput);
    }


    @Test
    public void shouldProcessRecordsWithValidData()
            throws CharacterCodingException {

        String someTestData = "foobar";
        List<KinesisClientRecord> records = makeRecordList(someTestData, UTF_8);
        setupProcessRecordsInputStubs(records);

        processorSpy.processRecords(processRecordsInput);

        then(processorSpy).should().processRecords(processRecordsInput);
    }


    private List<KinesisClientRecord> makeRecordList(String someTestData, Charset charset)
            throws CharacterCodingException {

        CharsetEncoder encoder = Charset.forName(charset.name()).newEncoder();
        CharBuffer charBuffer = CharBuffer.wrap(someTestData.toCharArray());
        ByteBuffer buffer = encoder.encode(charBuffer);

        KinesisClientRecord dataRecord = KinesisClientRecord.builder().data(buffer).build();
        List<KinesisClientRecord> records = new ArrayList();
        records.add(dataRecord);

        return records;
    }


    private void setupProcessRecordsInputStubs(List<KinesisClientRecord> records) {
        given(processRecordsInput.records()).willReturn(records);
        given(processRecordsInput.checkpointer()).willReturn(checkpointer);

        willCallRealMethod().given(processorSpy).processRecords(processRecordsInput);
    }


    @Test
    public void expectCharacterCodingExceptionWhenProcessRecords()
            throws CharacterCodingException {

        String someTestData = "Olé";

        // Use ISO_8859_1 to cause a CharacterCodingException to occur in
        // AmazonKinesisApplicationRecordProcessor.processRecords();
        Charset charsetToCauseException = ISO_8859_1;

        List<KinesisClientRecord> records =
            makeRecordList(someTestData, charsetToCauseException);
        setupProcessRecordsInputStubs(records);

        processorSpy.processRecords(processRecordsInput);

        then(processorSpy).should().processRecords(processRecordsInput);
    }
}
