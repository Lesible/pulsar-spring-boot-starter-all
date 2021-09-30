package com.sumwhy.pulsar.autoconfigure;

import com.sumwhy.pulsar.properties.GlobalConsumerProperties;
import com.sumwhy.pulsar.properties.PulsarProperties;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * <p> @date: 2021-04-06 15:52</p>
 *
 * @author 何嘉豪
 */
@Configuration
@ComponentScan(basePackages = "io.lesible")
@ConditionalOnProperty(name = "pulsar.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({PulsarProperties.class, GlobalConsumerProperties.class})
public class PulsarAutoConfiguration {

    private final PulsarProperties pulsarProperties;

    public PulsarAutoConfiguration(PulsarProperties pulsarProperties) {
        this.pulsarProperties = pulsarProperties;
    }

    @Bean("pulsarClient")
    @ConditionalOnProperty(name = "pulsar.is-tdmq", havingValue = "false", matchIfMissing = true)
    public PulsarClient pulsarClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarProperties.getServiceUrl())
                .ioThreads(pulsarProperties.getIoThreads())
                .listenerThreads(pulsarProperties.getListenerThreads())
                .connectionsPerBroker(pulsarProperties.getConnectionsPerBroker())
                .keepAliveInterval((int) pulsarProperties.getKeepAliveInterval().getSeconds(), TimeUnit.SECONDS)
                .connectionTimeout((int) pulsarProperties.getConnectionTimeout().getSeconds(), TimeUnit.SECONDS)
                .operationTimeout((int) pulsarProperties.getOperationTimeout().getSeconds(), TimeUnit.SECONDS)
                .startingBackoffInterval(pulsarProperties.getMaxBackoffInterval().toMillis(), TimeUnit.MILLISECONDS)
                .maxBackoffInterval(pulsarProperties.getMaxBackoffInterval().toMillis(), TimeUnit.SECONDS);
        String token = pulsarProperties.getToken();
        if (StringUtils.hasText(token)) {
            clientBuilder.authentication(AuthenticationFactory.token(token));
        }
        return clientBuilder.build();
    }

    @Bean(name = "pulsarClient")
    @ConditionalOnProperty(name = "pulsar.is-tdmq", havingValue = "true")
    public PulsarClient tdmqClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(pulsarProperties.getServiceUrl())
                .ioThreads(pulsarProperties.getIoThreads())
                .authentication(AuthenticationFactory.token(pulsarProperties.getToken()))
                .listenerName(String.format("custom:%s", pulsarProperties.getRouteId()))
                .listenerThreads(pulsarProperties.getListenerThreads())
                .connectionsPerBroker(pulsarProperties.getConnectionsPerBroker())
                .keepAliveInterval((int) pulsarProperties.getKeepAliveInterval().getSeconds(), TimeUnit.SECONDS)
                .connectionTimeout((int) pulsarProperties.getConnectionTimeout().getSeconds(), TimeUnit.SECONDS)
                .operationTimeout((int) pulsarProperties.getOperationTimeout().getSeconds(), TimeUnit.SECONDS)
                .startingBackoffInterval(pulsarProperties.getMaxBackoffInterval().toMillis(), TimeUnit.MILLISECONDS)
                .maxBackoffInterval(pulsarProperties.getMaxBackoffInterval().toMillis(), TimeUnit.SECONDS)
                .build();
    }

}

