package org.thingsboard.gateway.extensions.kinesis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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

    private List<Kinesis> kinesisStreams;


    @PostConstruct
    public void init() throws Exception {
        log.info("Initializing Kinesis service!");

        KinesisConfiguration configuration = configureService();

        initializeService(configuration);
    }


    protected KinesisConfiguration configureService() throws Exception {
        KinesisConfiguration configuration;

        try {
            configuration = ConfigurationTools.readFileConfiguration(configurationFile, KinesisConfiguration.class);
        } catch (Exception e) {
            log.error("Kinesis service configuration failed!", e);
            throw e;
        }

        return configuration;
    }


    protected void initializeService(KinesisConfiguration configuration) {
        try {
            Stream<KinesisStreamConfiguration> configurationStream =
                configuration.getKinesisStreamConfigurations().stream();

            kinesisStreams =
                configurationStream.map(config ->
                    buildKinesis(service, config)).collect(Collectors.toList()
                );

            kinesisStreams.forEach(Kinesis::init);

        } catch (Exception e) {
            log.error("Kinesis service initialization failed!", e);
            throw e;
        }
    }


    protected Kinesis buildKinesis(GatewayService service, KinesisStreamConfiguration config) {
        return new Kinesis(service, config);
    }


    @PreDestroy
    public void preDestroy() {
        if (kinesisStreams != null) {
            kinesisStreams.forEach(Kinesis::stop);
        }
    }
}
