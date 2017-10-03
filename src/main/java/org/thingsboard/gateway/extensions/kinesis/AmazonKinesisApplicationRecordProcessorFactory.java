package org.thingsboard.gateway.extensions.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
/**
 * Used to create new record processors.
 */
public class AmazonKinesisApplicationRecordProcessorFactory implements IRecordProcessorFactory {


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
    public IRecordProcessor createProcessor() {
        return new AmazonKinesisApplicationRecordProcessor(extension);
    }
}