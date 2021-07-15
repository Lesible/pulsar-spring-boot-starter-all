package io.lesible.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lesible.annotation.DeadLetter;
import io.lesible.annotation.PulsarConsumer;
import io.lesible.model.User;
import io.lesible.producer.IProducerFactory;
import io.lesible.producer.ProducerFactory;
import io.lesible.producer.ProducerHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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

    @Resource
    private PulsarClient pulsarClient;

    @Bean
    @Lazy(false)
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

    @Bean
    public IProducerFactory producerFactory() throws Exception {
        return new ProducerFactory()
                .addProducer("batch-topic", ProducerHolder.builder()
                        .batchingMaxMessages(20)
                        .batchingMaxPublishDelay(Duration.ofSeconds(30))
                        .producerName("batch-user-producer")
                        .enableBatching(true)
                        .build())
                .addProducer("delay-after-topic")
                .addProducer("delay-at-topic");
    }

    @PulsarConsumer(topic = "batch-topic",
            deadLetter = @DeadLetter(maxRedeliverCount = 20,
                    deadLetterTopic = "dead-batch-topic", retryLetterTopic = "retry-batch-topic"),
            consumerName = "batch-user-consumer")
    public void batchConsumeUser(byte[] bytes) throws Exception {
        User user = new ObjectMapper().readValue(new String(bytes, StandardCharsets.UTF_8), User.class);
        log.info("user: {}", user);
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
    public void userTopicConsumer(User user) throws Exception {
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }

    @PulsarConsumer(topic = "simple-topic")
    public void simpleTopicConsumer(String msg) throws Exception {
        log.info("simple msg received:{},at {}", msg, LocalDateTime.now());
    }

}

