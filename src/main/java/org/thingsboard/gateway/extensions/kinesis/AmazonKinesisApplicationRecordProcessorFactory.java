package org.thingsboard.gateway.extensions.kinesis;


import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;


/**
 * Used to create new record processors.
 */
public class AmazonKinesisApplicationRecordProcessorFactory implements ShardRecordProcessorFactory {


	private Kinesis extension;

	public AmazonKinesisApplicationRecordProcessorFactory(Kinesis extension)
	{
		super();
		this.extension = extension;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new AmazonKinesisApplicationRecordProcessor(extension);
    }
}

