package org.thingsboard.gateway.extensions.kinesis;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

import com.amazonaws.services.kinesis.model.Record;

import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Future;


/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationRecordProcessor implements IRecordProcessor {

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


    private static final int OPERATION_TIMEOUT_IN_SEC = 10;
    private static final int MAX_FUTURES = 5;

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
        this.kinesisShardId = initializationInput.getShardId();
        LOG.info("Initializing record processor for shard: " + this.kinesisShardId );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {

        List<Record> records = processRecordsInput.getRecords();
        IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        
        try {
            processRecordsWithRetries(records);

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(checkpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        }
        catch ( Exception e ) {
            LOG.error("Couldn't process record batch.", e);
        }
    }

    public class RecordFuture {
        public RecordFuture(Record record, ArrayList<Future> futures) {
            this.record = record;
            this.futures = futures;
        }

        public Record record;
        public ArrayList<Future> futures;
        public int retryCount = 0;
    }

    public class ProcessRecordException extends Exception {
        public Record record;

        public ProcessRecordException(Record record) {
            this.record = record;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) throws Exception {
        LinkedList<RecordFuture> futureRecords = new LinkedList<RecordFuture>();

        int recordInvoked = 0;
        int recordResolved = 0;

        int futureInvoked = 0;
        int futureRetried = 0;
        int futureResolved = 0;

        for (Record record : records) {
            //
            // Logic to process record goes here.
            //
            recordInvoked += 1;
            futureRecords.add(new RecordFuture(record, processSingleRecord(record)));

            while ( futureRecords.size() > MAX_FUTURES ) {
                RecordFuture recordFuture = futureRecords.pollFirst();
                recordResolved += 1;
                futureInvoked += recordFuture.futures.size();
                for (int j = 0; j < recordFuture.futures.size(); j++) {
                    try {
                        recordFuture.futures.get(j).get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
                        futureResolved += 1;
                    } catch (Throwable t) {
                        LOG.error("Retrying record.", t);
                        if (recordFuture.retryCount++ >= NUM_RETRIES) {
                            // backoff if we encounter an exception.
                            try {
                                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                            } catch (InterruptedException e) {
                                LOG.debug("Interrupted sleep", e);
                            }
                           
                            futureRetried += 1;
                            futureRecords.add(new RecordFuture(recordFuture.record, processSingleRecord(recordFuture.record)));
                        } else {
                            LOG.error("Couldn't process record " + recordFuture.record + ". Skipping the record.");
                            throw new ProcessRecordException(recordFuture.record);
                        }
                    }
                }
            }
        }

        RecordFuture recordFuture = futureRecords.pollFirst();
        while (recordFuture != null) {
            recordResolved += 1;
            futureInvoked += recordFuture.futures.size();
            for (int j = 0; j < recordFuture.futures.size(); j++) {
                try {
                    recordFuture.futures.get(j).get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
                    futureResolved += 1;

                } catch (Throwable t) {
                    if (recordFuture.retryCount++ >= NUM_RETRIES) {

                        // backoff if we encounter an exception.
                        try {
                            Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                        } catch (InterruptedException e) {
                            LOG.debug("Interrupted sleep", e);
                        }
                       
                        futureRetried += 1;
                        futureRecords.add(new RecordFuture(recordFuture.record, processSingleRecord(recordFuture.record)));    
                    } else {
                        LOG.error("Couldn't process record " + recordFuture.record + ". Skipping the record.");
                        throw new ProcessRecordException(recordFuture.record);
                    }
                }
            }

            recordFuture = futureRecords.pollFirst();
        }

        LOG.info("records: " + records.size());
        LOG.info("recordInvoked: " + recordInvoked);
        LOG.info("recordResolved: " + recordResolved);
        LOG.info("futureInvoked: " + futureInvoked);
        LOG.info("futureRetried: " + futureRetried);
        LOG.info("futureResolved: " + futureResolved);
    }

    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private ArrayList<Future> processSingleRecord (Record record) throws Exception {
        String data = decoder.decode(record.getData()).toString();
        return extension.processBody(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        ShutdownReason reason = shutdownInput.getShutdownReason();

        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
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
}
