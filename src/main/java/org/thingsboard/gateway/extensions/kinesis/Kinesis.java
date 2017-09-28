package org.thingsboard.gateway.extensions.kinesis;

import lombok.extern.slf4j.Slf4j;

import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.JsonTools;

@Slf4j
public class Kinesis {

    //private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    private GatewayService gateway;
    private KinesisConfiguration configuration;
    //private Thread reader;

    public Kinesis(GatewayService service, KinesisConfiguration c) {
        this.gateway = service;
        this.configuration = c;
    }

    public void init() {
        
        //init reader

        
        log.info("GOT HERE {}", this.configuration.file);

    }

    public void stop() {
        /*if (reader != null) {
            reader.interrupt();
        }*/
    }

    private void processBody(String body, KinesisConfiguration configuration) throws Exception {
        

    }

    
}
