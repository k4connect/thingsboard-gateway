package org.thingsboard.gateway.extensions.kinesis;

import static software.amazon.awssdk.regions.Region.US_EAST_1;
import static software.amazon.kinesis.common.InitialPositionInStream.LATEST;

import lombok.extern.slf4j.Slf4j;

import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.JsonTools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import java.net.InetAddress;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.thingsboard.server.common.data.kv.*;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;


@Slf4j
public class Kinesis {
    public static final String EVENTS_STARTED_PATH = "/events/started";

    private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    private static final String APPLICATION_NAME = "ThingsboardKinesisApplication";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream APPLICATION_INITIAL_POSITION_IN_STREAM = LATEST;


    private GatewayService gateway;
    private KinesisStreamConfiguration configuration;

    private KinesisAsyncClient kinesisClient;
    private DynamoDbAsyncClient dynamoClient;
    private CloudWatchAsyncClient cloudWatchClient;

    // Use default (package) scope so unit tests can insert mock object
    Scheduler scheduler = null;

    protected Region region = US_EAST_1;

    private static AwsCredentialsProvider credentialsProvider;


    public Kinesis(GatewayService service, KinesisStreamConfiguration c) {
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
        credentialsProvider = ProfileCredentialsProvider.create();

        try {
            credentialsProvider.resolveCredentials();
        } catch (Exception e) {
            String message =
                "Cannot load the credentials from the credential profiles file. " +
                "Please make sure that your credentials file is at the correct " +
                "location (~/.aws/credentials), and is in valid format.";

            throw SdkException.builder().message(message).cause(e).build();
        }

        String workerId;

        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (Exception e) {
            throw SdkException.builder().message("Cannot create workerId").build();
        }

        initializeClients();
        // initializeWorker(workerId);
        initializeScheduler();

        log.info("Running {} to process stream {} as worker {}...",
                APPLICATION_NAME,
                configuration.getStream(),
                workerId);

        // worker.run();
        runScheduler();
    }


    // // Amazon KCL v1.8.5 way of doing this
    // private void initializeWorker(String workerId) {
    //     KinesisClientLibConfiguration kinesisClientLibConfiguration =
    //             new KinesisClientLibConfiguration(APPLICATION_NAME,
    //                     configuration.getStream(),
    //                     credentialsProvider,
    //                     workerId);
    //     kinesisClientLibConfiguration.withInitialPositionInStream(APPLICATION_INITIAL_POSITION_IN_STREAM);

    //     IRecordProcessorFactory recordProcessorFactory =
    //         new AmazonKinesisApplicationRecordProcessorFactory(this);

    //     if (worker == null) {
    //         worker = new Worker.Builder()
    //             .recordProcessorFactory(recordProcessorFactory)
    //             .config(kinesisClientLibConfiguration)
    //             .build();
    //     }
    // }


    protected void initializeClients() {
        kinesisClient =
            KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
        dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
    }


    protected void initializeScheduler() {
        ConfigsBuilder configsBuilder = makeConfigsBuilder();

        makeScheduler(configsBuilder);
    }


    protected ConfigsBuilder makeConfigsBuilder() {
        AmazonKinesisApplicationRecordProcessorFactory recordProcessorFactory =
            new AmazonKinesisApplicationRecordProcessorFactory(this);

        ConfigsBuilder builder =
            new ConfigsBuilder(configuration.getStream(), APPLICATION_NAME,
                    kinesisClient, dynamoClient, cloudWatchClient,
                    UUID.randomUUID().toString(), recordProcessorFactory);

        return builder;
    }


    protected void makeScheduler(ConfigsBuilder configsBuilder) {
        if (scheduler == null) {
            scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
            );
        }
    }


    // TODO: April 8, 2019: This method still needs to be implemented!
    protected void runScheduler() {

    }

    // TODO: April 8, 2019: This method needs to be examined for changes
    public void stop() {
        // worker.shutdown();
    }


    // needs to be called from inside KCL
    public void processBody(String body) {

        ObjectMapper mapper = new ObjectMapper();

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        KinesisMessage message;


        try {
            message = mapper.readValue(body, KinesisMessage.class);
        } catch (Exception e) {
            log.error("Failed to parse message body. {}", e);
            return;
        }

        try {
            MqttDeliveryFuture future1 = parseVariablesEvents(message);
            MqttDeliveryFuture future2 = parseController(message);

            waitWithTimeout(future1);
            if (future2 != null) {
                waitWithTimeout(future2);
            }
        } catch (Exception e) {
            log.error("Failed to send. Body: {} Exception: {}", body, e);
        }

    }


    private MqttDeliveryFuture parseController(KinesisMessage message) throws Exception {

        List<TsKvEntry> telemetry = new ArrayList<>();

        if (isControllerDisconnectMessage(message)) {
            // controller disconnect
            BooleanDataEntry data = new BooleanDataEntry("online", false);
            telemetry.add(new BasicTsKvEntry(message.timestamp, data));

        } else {
            BooleanDataEntry data = new BooleanDataEntry("online", true);
            telemetry.add(new BasicTsKvEntry(message.timestamp, data));

            StringDataEntry lastData = new StringDataEntry("lastData", Long.toString(message.timestamp));
            telemetry.add(new BasicTsKvEntry(message.timestamp, lastData));
        }

        return gateway.onDeviceTelemetry(message.analyticsId, telemetry);
    }


    private boolean isControllerDisconnectMessage(KinesisMessage message) {
        return (message.oid != null &&
                message.oid.equals("1.3.6.1.4.1.32473.1.2") &&
                message.type != null &&
                message.type.equals("disconnect"));
    }


    private MqttDeliveryFuture parseVariablesEvents(KinesisMessage message) throws Exception {
        MqttDeliveryFuture future = null;

        // skip anything without a path, and everything not in devices
        if (isDeviceMessage(message)) {
            if (isVariablesMessage(message)) {
                String variable = message.path.substring(message.path.indexOf("/variables/") + 11);
                String device = message.analyticsId + "/" + message.path.replace("/variables/" + variable, "");

                future = postTelemetry(device, variable, message.value, message.timestamp);

            } else if (isEventsMessage(message)) {
                if (isValidStartedEvent(message)) {
                    String device =
                        message.analyticsId + "/" + message.path.replace(EVENTS_STARTED_PATH, "");
                    future = postTelemetry(device, "started",  Long.toString(message.timestamp), message.timestamp);
                }

                // Devices/Living Room/Controller/Bridges/Zwave/events/started
                log.info("Path: {} Type: {} Value: {}", message.path, message.type, message.value);
            }
        }

        return future;
    }


    private boolean isDeviceMessage(KinesisMessage message) {
        return (message.path != null &&
                !message.path.isEmpty() &&
                message.path.indexOf("Devices") == 0);
    }


    private boolean isVariablesMessage(KinesisMessage message) {
        return message.path.contains("variables");
    }


    private boolean isEventsMessage(KinesisMessage message) {
        return message.path.contains("events");
    }


    private boolean isValidStartedEvent(KinesisMessage message) {
        boolean isValidStartedDevice =
            message.path.contains("Bridges") || message.path.contains("Servers");

        return isValidStartedDevice && message.path.contains(EVENTS_STARTED_PATH);
    }


    private MqttDeliveryFuture postTelemetry(String device, String variable, String value, Long timestamp) throws Exception {
        StringDataEntry data = new StringDataEntry(variable, value);

        List<TsKvEntry> telemetry = new ArrayList<>();
        telemetry.add(new BasicTsKvEntry(timestamp, data));

        return gateway.onDeviceTelemetry(device, telemetry);
    }


    private void waitWithTimeout(Future future) throws Exception {
        future.get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
}
