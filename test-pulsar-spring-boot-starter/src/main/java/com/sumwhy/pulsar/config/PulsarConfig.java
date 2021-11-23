package com.sumwhy.pulsar.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sumwhy.pulsar.annotation.PulsarConsumer;
import com.sumwhy.pulsar.model.ProducerHolder;
import com.sumwhy.pulsar.model.User;
import com.sumwhy.pulsar.producer.IProducerFactory;
import com.sumwhy.pulsar.producer.ProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * <p> @date: 2021-04-07 14:42</p>
 *
 * @author 何嘉豪
 */
@Slf4j
@Component
public class PulsarConfig {
/*

    @Resource
    private PulsarClient pulsarClient;

    @Bean
    public LazyInitializationExcludeFilter integrationLazyInitExcludeFilter() {
        return LazyInitializationExcludeFilter.forBeanTypes(IProducerFactory.class, PulsarClient.class, Producer.class);
    }


    @Bean
    public Producer<byte[]> producer() throws Exception {
        return pulsarClient.newProducer().topic("simple-topic")
                .producerName("simple-producer").create();
    }

    @Bean
    public Producer<byte[]> produceUser() throws Exception {
        return pulsarClient.newProducer().topic("user-topic").producerName("user-producer").create();
    }
*/

    @Bean
    public IProducerFactory producerFactory() throws Exception {
        return new ProducerFactory()
                .addProducer(ProducerHolder.builder("batch-topic")
                        .batchingMaxMessages(20)
                        .batchingMaxPublishDelay(Duration.ofSeconds(30))
                        .producerName("batch-user-producer")
                        .enableBatching(true)
                        .build())
                .addProducer("delay-after-topic")
                .addProducer("delay-at-topic")
                .addProducer("user-topic");
    }


    @PulsarConsumer(topic = "delay-after-topic")
    public void delayAfterConsumer(byte[] bytes) throws Exception {
        User user = new ObjectMapper().readValue(new String(bytes, StandardCharsets.UTF_8), User.class);
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }

    @PulsarConsumer(topic = "delay-at-topic")
    public void delayAtConsumer(byte[] bytes) throws Exception {
        User user = new ObjectMapper().readValue(new String(bytes, StandardCharsets.UTF_8), User.class);
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }

    @PulsarConsumer(topic = "user-topic")
    public void userTopicConsumer(User user, Consumer<?> consumer, Message<byte[]> message) throws Exception {
        log.info("consumer: {}", consumer);
        log.info("message: {}", message);
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }

    @PulsarConsumer(topic = "simple-topic", namespace = "default", tenant = "public")
    public void simpleTopicConsumer(String msg) throws Exception {
        log.info("simple msg received:{},at {}", msg, LocalDateTime.now());
    }

}

