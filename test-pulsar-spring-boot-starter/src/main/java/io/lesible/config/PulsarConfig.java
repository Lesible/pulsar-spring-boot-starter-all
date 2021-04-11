package io.lesible.config;

import io.lesible.annotation.DeadLetter;
import io.lesible.annotation.PulsarConsumer;
import io.lesible.model.User;
import io.lesible.producer.IProducerFactory;
import io.lesible.producer.ProducerFactory;
import io.lesible.producer.ProducerHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * <p> @date: 2021-04-07 14:42</p>
 *
 * @author 何嘉豪
 */
@Slf4j
@Configuration
public class PulsarConfig {

    @Resource
    private PulsarClient pulsarClient;

    @Bean
    public Producer<byte[]> producer() throws Exception {
        return pulsarClient.newProducer().topic("simple-topic")
                .producerName("simple-producer").create();
    }

    @Bean
    public Producer<User> produceUser() throws Exception {
        return pulsarClient.newProducer(Schema.JSON(User.class))
                .topic("user-topic").producerName("user-producer").create();
    }

    @Bean
    public IProducerFactory producerFactory() throws Exception {
        return new ProducerFactory()
                .addProducer("batch-topic", ProducerHolder.builder()
                        .batchingMaxMessages(20)
                        .batchingMaxPublishDelay(Duration.ofSeconds(30))
                        .producerName("batch-user-producer")
                        .enableBatching(true)
                        .msgType(User.class)
                        .build())
                .addProducer("delay-after-topic", User.class)
                .addProducer("delay-at-topic", User.class);
    }

    @PulsarConsumer(topic = "batch-topic", msgType = User.class,
            deadLetter = @DeadLetter(maxRedeliverCount = 20,
                    deadLetterTopic = "dead-batch-topic", retryLetterTopic = "retry-batch-topic"),
            consumerName = "batch-user-consumer")
    public void batchConsumeUser(User user) {
        log.info("user: {}", user);
    }


    @PulsarConsumer(topic = "delay-after-topic", msgType = User.class)
    public void delayAfterConsumer(User user) {
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }

    @PulsarConsumer(topic = "delay-at-topic", msgType = User.class)
    public void delayAtConsumer(User user) {
        log.info("user: {},date: {}", user, LocalDateTime.now());
    }
}
