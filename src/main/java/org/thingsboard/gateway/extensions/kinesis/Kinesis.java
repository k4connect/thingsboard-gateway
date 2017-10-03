package org.thingsboard.gateway.extensions.kinesis;

import lombok.extern.slf4j.Slf4j;

import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.JsonTools;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.thingsboard.server.common.data.kv.*;


@Slf4j
public class Kinesis {

    //private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    private GatewayService gateway;
    private KinesisConfiguration configuration;

    private Worker worker;

    private static final int OPERATION_TIMEOUT_IN_SEC = 10;
    
    private static final String APPLICATION_NAME = "ThingsboardKinesisApplication";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream APPLICATION_INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;

    private static AWSCredentialsProvider credentialsProvider;

    public Kinesis(GatewayService service, KinesisConfiguration c) {
        this.gateway = service;
        this.configuration = c;
    }

    public void init() {
        
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }

        String workerId;

        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot create workerId");
        }
        

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(APPLICATION_NAME,
                        configuration.getStream(),
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(APPLICATION_INITIAL_POSITION_IN_STREAM);

        IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory(this);

        worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kinesisClientLibConfiguration).build();

        log.info("Running {} to process stream {} as worker {}...",
                APPLICATION_NAME,
                configuration.getStream(),
                workerId);

        worker.run();

    }

    public void stop() {
        
        worker.shutdown();

    }

    //needs to be called from inside KCL
    public void processBody(String body) {

        ObjectMapper mapper = new ObjectMapper();
        KinesisMessage message;



        try {
            message = mapper.readValue(body, KinesisMessage.class);
        } catch (Exception e)
        {
            log.error("Failed to parse message body. {}", e);
            return; 
        }

        //TESTING
        if(!message.analyticsId.equals("d2cb6b5e-2745-4ce2-b5b2-2dec4b42f49c"))
        {
            return;
        }

        //log.info(body);

        
        try {
            parseVariablesEvents(message);
            parseController(message);
        } catch (Exception e) {
            log.error("Failed to send. Body: {} Exception: {}", body, e);
        }


        
 

        //attributes - serial no, etc


        
        log.info("KinesisMessage {}/{}", message.analyticsId, message.path);
    }


    private void parseController(KinesisMessage message) throws Exception
    {

        List<TsKvEntry> telemetry = new ArrayList<>();

        LongDataEntry lastData = new LongDataEntry("lastData", message.timestamp);
        telemetry.add(new BasicTsKvEntry(message.timestamp, lastData));


        if(message.oid != null && message.oid.equals("1.3.6.1.4.1.32473.1.2") )
        {
            //from the controller

            if(message.type != null && message.type.equals("connect"))
            {

                BooleanDataEntry data = new BooleanDataEntry("online", true);
                telemetry.add(new BasicTsKvEntry(message.timestamp, data));

                waitWithTimeout(gateway.onDeviceConnect(message.analyticsId, "Controller"));

            } else if(message.type != null && message.type.equals("disconnect"))
            {
                BooleanDataEntry data = new BooleanDataEntry("online", false);
                telemetry.add(new BasicTsKvEntry(message.timestamp, data));

                waitWithTimeout(gateway.onDeviceDisconnect(message.analyticsId).get());
                
                
            }

        }

        waitWithTimeout(gateway.onDeviceTelemetry(message.analyticsId, telemetry));

    }

    private void parseVariablesEvents(KinesisMessage message) throws Exception
    {
        if(message.path != null && !message.path.isEmpty())
        {

            if(message.path.contains("variables"))
            {
                String variable = message.path.substring(message.path.indexOf("/variables/") + 11);
                String device = message.analyticsId + "/" + message.path.replace("/variables/" + variable, "");
                
                postTelemetry(device, variable, message.value, message.timestamp);


            } else if(message.path.contains("events"))
            {
                
            }
        }
    }


    private void postTelemetry(String device, String variable, String value, Long timestamp) throws Exception
    {
        StringDataEntry data = new StringDataEntry(variable, value);

        List<TsKvEntry> telemetry = new ArrayList<>();
        telemetry.add(new BasicTsKvEntry(timestamp, data));

        waitWithTimeout(gateway.onDeviceTelemetry(device, telemetry));
    }

    private void waitWithTimeout(Future future) throws Exception {
        future.get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }

    
}
