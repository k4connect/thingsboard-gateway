package org.thingsboard.gateway.extensions.kinesis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 15.05.17.
 */
@Service
@ConditionalOnProperty(prefix = "kinesis", value = "enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class DefaultKinesisService {
    @Autowired
    private GatewayService service;

    @Value("${kinesis.configuration}")
    private String configurationFile;

    private List<Kinesis> brokers;

    @PostConstruct
    public void init() throws Exception {
        log.info("Initializing Kinesis service!");
        KinesisConfiguration configuration;
        try {
            configuration = ConfigurationTools.readFileConfiguration(configurationFile, KinesisConfiguration.class);
        } catch (Exception e) {
            log.error("Kinesis service configuration failed!", e);
            throw e;
        }

        try {

            brokers = configuration.getKinesisStreamConfigurations().stream().map(c -> new Kinesis(service, c)).collect(Collectors.toList());
            brokers.forEach(Kinesis::init);
       
        } catch (Exception e) {
            log.error("Kinesis service initialization failed!", e);
            throw e;
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (brokers != null) {
            brokers.forEach(Kinesis::stop);
        }
    }

}
