package org.thingsboard.gateway.extensions.kinesis;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationRecordProcessor implements ShardRecordProcessor {

    private static final Log LOG = LogFactory.getLog(AmazonKinesisApplicationRecordProcessor.class);
    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private Kinesis extension;


    public  AmazonKinesisApplicationRecordProcessor(Kinesis extension)
    {
        super();
        this.extension = extension;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput initializationInput) {
        this.kinesisShardId = initializationInput.shardId();

        LOG.info("Initializing record processor for shard: " + this.kinesisShardId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<KinesisClientRecord> records = processRecordsInput.records();
        RecordProcessorCheckpointer checkpointer = processRecordsInput.checkpointer();

        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }


    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<KinesisClientRecord> records) {
        for (KinesisClientRecord record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }


    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(KinesisClientRecord record) {
        String data = null;

        try {
            data = decoder.decode(record.data()).toString();
            extension.processBody(data);
        } catch (CharacterCodingException e) {
            LOG.error("Malformed data: " + data, e);
        }
    }


    /**
     * {@inheritDoc}
     */
// Replaced by shardEnded, but leaving in place for now because some of the
// Amazon example logic has changed from the K4Connect implementation.
    // @Override
    // public void shutdown(ShutdownInput shutdownInput) {
    //     LOG.info("Shutting down record processor for shard: " + kinesisShardId);
    //     // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
    //     ShutdownReason reason = shutdownInput.getShutdownReason();

    //     if (reason == ShutdownReason.TERMINATE) {
    //         checkpoint(shutdownInput.getCheckpointer());
    //     }
    // }


    /**
     * {@inheritDoc}
     */
    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        // TODO: April 9, 2019: Not sure if this needs an impelementation or
        // not; the examples in the Amazon KCL migration guide don't show an
        // implementation, but they also don't show implementations for other
        // methods that we have already implemented.
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        checkpoint(shardEndedInput.checkpointer());
    }


    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        checkpoint(shutdownRequestedInput.checkpointer());
    }
}
