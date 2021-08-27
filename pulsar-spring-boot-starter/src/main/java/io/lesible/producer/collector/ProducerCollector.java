package io.lesible.producer.collector;

import io.lesible.annotation.PulsarProducer;
import io.lesible.exception.InitFailedException;
import io.lesible.producer.IProducerFactory;
import io.lesible.producer.ProducerHolder;
import io.lesible.util.SchemaUtil;
import io.lesible.util.TopicBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p> @date: 2021-04-07 15:35</p>
 *
 * @author 何嘉豪
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "pulsar.enabled", havingValue = "true", matchIfMissing = true)
public class ProducerCollector implements BeanPostProcessor, EmbeddedValueResolverAware {

    /**
     * topic 和 producer 的映射
     */
    private final Map<String, Producer<?>> producerMapping = new ConcurrentHashMap<>();

    private final PulsarClient pulsarClient;

    private final TopicBuilder topicBuilder;

    private StringValueResolver stringValueResolver;

    public ProducerCollector(PulsarClient pulsarClient, TopicBuilder topicBuilder) {
        this.pulsarClient = pulsarClient;
        this.topicBuilder = topicBuilder;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();
        if (beanClass.isAnnotationPresent(PulsarProducer.class) && bean instanceof IProducerFactory) {
            producerMapping.putAll(((IProducerFactory) bean).getProducersInfo().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> createProducer(entry.getValue()))));
        }
        if (bean instanceof Producer) {
            Producer<?> producer = (Producer<?>) bean;
            String topic = producer.getTopic();
            // 此方法如果原键有值,将不会插入,并返回原值
            Producer<?> oldValue = producerMapping.putIfAbsent(topic, producer);
            // 如果同一个 topic 有两个 producer, 关闭旧的, 用新的覆盖
            if (oldValue != null) {
                try {
                    oldValue.close();
                } catch (PulsarClientException e) {
                    throw new RuntimeException("producer 关闭失败", e);
                }
                producerMapping.put(topic, producer);
            }
            log.debug("topic:{} 的生产者注册成功", topic);
        }
        return bean;
    }

    /**
     * 初始化 producer 对象
     *
     * @param producerHolder producer 信息
     * @return 生产者
     */
    private Producer<?> createProducer(ProducerHolder producerHolder) {
        String topic = stringValueResolver.resolveStringValue(producerHolder.getTopic());
        try {
            Schema<?> schema = SchemaUtil.schema(producerHolder.getMsgType());

            String producerName = producerHolder.getProducerName();

            if (!StringUtils.hasLength(producerName)) {
                producerName = topic + "-producer" + UUID.randomUUID();
            }
            String tenant = producerHolder.getTenant();
            String namespace = producerHolder.getNamespace();
            ProducerBuilder<?> builder = pulsarClient.newProducer(schema)
                    .topic(topicBuilder.buildTopicUrl(tenant, namespace, topic)).producerName(producerName)
                    .blockIfQueueFull(producerHolder.isBlockIfQueueFull())
                    .enableBatching(producerHolder.isEnableBatching())
                    .batchingMaxBytes(producerHolder.getBatchingMaxBytes())
                    .batchingMaxMessages(producerHolder.getBatchingMaxMessages());
            Duration sendTimeout = producerHolder.getSendTimeout();
            Duration batchingMaxPublishDelay = producerHolder.getBatchingMaxPublishDelay();
            if (sendTimeout != null) {
                builder.sendTimeout((int) sendTimeout.getSeconds(), TimeUnit.SECONDS);
            }
            if (batchingMaxPublishDelay != null) {
                builder.batchingMaxPublishDelay(batchingMaxPublishDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
            Producer<?> producer = builder.create();
            log.debug("初始化 topic 为 [{}] 的生产者成功", topic);
            return producer;
        } catch (PulsarClientException e) {
            log.error("初始化 topic 为 [{}] 的生产者失败", topic);
            throw new InitFailedException("初始化 topic 为 [" + topic + "] 的生产者失败", e);
        }
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }

    /**
     * 获取 producer 映射
     *
     * @return producer 映射
     */
    public Map<String, Producer<?>> getProducerMapping() {
        return producerMapping;
    }

    /**
     * 根据 topic 获取 producer
     *
     * @param topic topic
     * @return 生产者
     */
    @SuppressWarnings("rawtypes")
    public Optional<Producer> getProducer(String topic) {
        return Optional.ofNullable(producerMapping.get(topic));
    }
}
