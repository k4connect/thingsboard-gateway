package org.thingsboard.gateway.extensions.kinesis;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertNotNull;

import static org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfigurationTest.TEST_STREAM_NAME;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.thingsboard.gateway.extensions.kinesis.AmazonKinesisApplicationRecordProcessor;
import org.thingsboard.gateway.extensions.kinesis.AmazonKinesisApplicationRecordProcessorFactory;
import org.thingsboard.gateway.extensions.kinesis.Kinesis;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;

import software.amazon.kinesis.processor.ShardRecordProcessor;


@RunWith(MockitoJUnitRunner.class)
public class AmazonKinesisApplicationRecordProcessorFactoryTest {
    @Mock
    GatewayService gateway;

    private KinesisStreamConfiguration streamConfig = null;

    private Kinesis extension = null;


    @Before
    public void setup() {
        streamConfig = new KinesisStreamConfiguration();
        streamConfig.setStream(TEST_STREAM_NAME);

        extension = new Kinesis(gateway, streamConfig);
    }


    @Test
    public void shouldCreateFactoryInstance() {
        AmazonKinesisApplicationRecordProcessorFactory processorFactory =
            new AmazonKinesisApplicationRecordProcessorFactory(extension);

        assertNotNull(processorFactory);
        assertThat(processorFactory, isA(AmazonKinesisApplicationRecordProcessorFactory.class));
    }


    @Test
    public void shouldCreateInstanceOf_IRecordProcessor() {
        AmazonKinesisApplicationRecordProcessorFactory processorFactory =
            new AmazonKinesisApplicationRecordProcessorFactory(extension);

        ShardRecordProcessor processor = processorFactory.shardRecordProcessor();

        assertNotNull(processor);
        assertThat(processor, isA(ShardRecordProcessor.class));
    }


    @Test
    public void shouldCreateInstanceOf_AmazonKinesisAppicationRecordProcessor() {
        AmazonKinesisApplicationRecordProcessorFactory processorFactory =
            new AmazonKinesisApplicationRecordProcessorFactory(extension);

        AmazonKinesisApplicationRecordProcessor amazonProcessor =
            (AmazonKinesisApplicationRecordProcessor) processorFactory.shardRecordProcessor();

        assertNotNull(amazonProcessor);
        assertThat(amazonProcessor, isA(AmazonKinesisApplicationRecordProcessor.class));
    }

}
