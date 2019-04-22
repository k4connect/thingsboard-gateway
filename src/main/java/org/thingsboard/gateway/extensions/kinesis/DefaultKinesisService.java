package org.thingsboard.gateway.extensions.kinesis;

import static org.thingsboard.gateway.util.ConfigurationTools.readConfiguration;
import static org.thingsboard.gateway.util.ConfigurationTools.readFileConfiguration;

import lombok.extern.slf4j.Slf4j;

import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.extensions.ExtensionUpdate;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisConfiguration;
import org.thingsboard.gateway.extensions.kinesis.conf.KinesisStreamConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by ashvayka on 15.05.17.
 */
@Slf4j
public class DefaultKinesisService extends ExtensionUpdate {

    private final GatewayService gateway;
    private TbExtensionConfiguration currentConfiguration;
    private List<Kinesis> kinesisStreams;


    public DefaultKinesisService(GatewayService gateway) {
        this.gateway = gateway;
    }


    @Override
    public TbExtensionConfiguration getCurrentConfiguration() {
        return currentConfiguration;
    }


    protected DefaultKinesisService currentConfiguration(TbExtensionConfiguration configurationNode) {
        currentConfiguration = configurationNode;

        return this;
    }


    @Override
    public void init(TbExtensionConfiguration configurationNode, Boolean isRemote)
        throws Exception {

        currentConfiguration(configurationNode);

        log.info("[{}] Initializing Kinesis service", gateway.getTenantLabel());

        KinesisConfiguration configuration = configureService(isRemote);

        initializeService(configuration);
    }


    protected KinesisConfiguration configureService(Boolean isRemote)
        throws IOException {

        KinesisConfiguration kinesisConfiguration;

        try {
            if (isRemote) {
                kinesisConfiguration =
                    readConfiguration(currentConfiguration.getConfiguration(), KinesisConfiguration.class);
            } else {
                kinesisConfiguration =
                    readFileConfiguration(currentConfiguration.getExtensionConfiguration(), KinesisConfiguration.class);
            }
        } catch (IOException e) {
            log.error("[{}] Kinesis service configuration failed!", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }

        return kinesisConfiguration;
    }


    protected void initializeService(KinesisConfiguration kinesisConfiguration) {
        try {
            Stream<KinesisStreamConfiguration> configurationStream =
                kinesisConfiguration.getKinesisStreamConfigurations().stream();

            kinesisStreams =
                configurationStream.map(config ->
                    buildKinesis(gateway, config)).collect(Collectors.toList()
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


    @Override
    public void destroy() {
        if (kinesisStreams != null) {
            kinesisStreams.forEach(Kinesis::stop);
        }
    }
}
