package com.sumwhy.pulsar.consumer;

import com.sumwhy.pulsar.annotation.DeadLetter;
import com.sumwhy.pulsar.annotation.PulsarConsumer;
import com.sumwhy.pulsar.consumer.collector.ConsumerCollector;
import com.sumwhy.pulsar.exception.EmptyTopicException;
import com.sumwhy.pulsar.exception.InitFailedException;
import com.sumwhy.pulsar.properties.GlobalConsumerProperties;
import com.sumwhy.pulsar.util.SchemaUtil;
import com.sumwhy.pulsar.util.TopicBuilder;
import com.sumwhy.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p> @date: 2021-04-07 10:04</p>
 * <p> consumer 聚合 </p>
 *
 * @author 何嘉豪
 */
@Slf4j
@Configuration
@DependsOn({"pulsarClient", "consumerCollector"})
@ConditionalOnProperty(name = "pulsar.enabled", havingValue = "true", matchIfMissing = true)
public class ConsumerAggregator implements EmbeddedValueResolverAware {

    /**
     * 构造器注入, 消费者收集器
     */
    private final ConsumerCollector consumerCollector;

    /**
     * 构造器注入, pulsar client
     */
    private final PulsarClient pulsarClient;

    /**
     * 构造器注入, 全局消费者配置
     */
    private final GlobalConsumerProperties globalConsumerProperties;

    private final TopicBuilder topicBuilder;

    /**
     * 用于处理 spEL
     */
    private StringValueResolver stringValueResolver;

    private List<Consumer<?>> consumers;

    public ConsumerAggregator(ConsumerCollector consumerCollector, PulsarClient pulsarClient, GlobalConsumerProperties globalConsumerProperties, TopicBuilder topicBuilder) {
        this.consumerCollector = consumerCollector;
        this.pulsarClient = pulsarClient;
        this.globalConsumerProperties = globalConsumerProperties;
        this.topicBuilder = topicBuilder;
    }

    @PostConstruct
    public void init() {
        Map<String, ConsumerHolder> consumerHolderMapping = consumerCollector.getConsumerHolderMapping();
        consumers = consumerHolderMapping.entrySet().stream()
                .map(entry -> subscribe(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * 初始化 consumer
     *
     * @param consumerName   consumer 名称
     * @param consumerHolder consumer 信息
     * @return 消费者
     */
    private Consumer<?> subscribe(String consumerName, ConsumerHolder consumerHolder) {
        PulsarConsumer pulsarConsumer = consumerHolder.getPulsarConsumer();
        String topic = stringValueResolver.resolveStringValue(pulsarConsumer.topic());
        if (!StringUtils.hasLength(topic)) {
            throw new EmptyTopicException(consumerName);
        }
        try {
            // 判断是否需要重试, 默认是需要的,但对于腾讯云来说 死信和重试队列的名称需要自己指定
            boolean retryEnable = pulsarConsumer.retryEnable();
            String tenant = stringValueResolver.resolveStringValue(pulsarConsumer.tenant());
            String namespace = stringValueResolver.resolveStringValue(pulsarConsumer.namespace());
            String subscriptionName = StringUtils.hasLength(pulsarConsumer.subscriptionName()) ?
                    pulsarConsumer.subscriptionName() : "subscription_" + topic;
            Schema<?> schema = SchemaUtil.schema(pulsarConsumer.msgType());
            ConsumerBuilder<?> consumerBuilder = pulsarClient.newConsumer(schema)
                    .consumerName(consumerName).topic(topicBuilder.buildTopicUrl(tenant, namespace, topic))
                    .subscriptionType(pulsarConsumer.subscriptionType())
                    .subscriptionName(subscriptionName).enableRetry(retryEnable);
            if (retryEnable) {
                // 获取死信队列策略,并进行设置
                DeadLetter deadLetter = pulsarConsumer.deadLetter();
                int maxRedeliverCount = deadLetter.maxRedeliverCount();
                String deadLetterTopic = deadLetter.deadLetterTopic();
                String retryLetterTopic = deadLetter.retryLetterTopic();
                DeadLetterPolicy deadLetterPolicy;
                if (!StringUtils.hasLength(deadLetterTopic)
                        && !StringUtils.hasLength(retryLetterTopic)) {
                    // 对于没有设置的场景, pulsar 将会在 retryEnable 时,自动初始化一个默认的死信策略
                    String prefix;
                    if (pulsarConsumer.createDeadLetterByAdmin()) {
                        prefix = topicBuilder.getPrefix(tenant, namespace) + "subscription_" + topic;
                    } else {
                        prefix = topicBuilder.getPrefix(tenant, namespace) + topic;
                    }
                    String deadQueueSuffix = topicBuilder.getDeadQueueSuffix();
                    String retryQueueSuffix = topicBuilder.getRetryQueueSuffix();
                    String lowercaseStr = pulsarConsumer.lowercase();
                    if (StringUtils.hasLength(lowercaseStr)) {
                        // 只有等于 true 是 返回 true （ignoreCase）
                        boolean lowercase = Boolean.parseBoolean(lowercaseStr);
                        deadQueueSuffix = lowercase ? deadQueueSuffix.toLowerCase() : deadQueueSuffix.toUpperCase();
                        retryQueueSuffix = lowercase ? retryQueueSuffix.toLowerCase() : retryQueueSuffix.toUpperCase();
                    }
                    deadLetterPolicy = DeadLetterPolicy.builder()
                            .deadLetterTopic(prefix + deadQueueSuffix)
                            .retryLetterTopic(prefix + retryQueueSuffix)
                            .maxRedeliverCount(maxRedeliverCount).build();
                } else {
                    DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();
                    builder.maxRedeliverCount(maxRedeliverCount);
                    if (StringUtils.hasLength(retryLetterTopic)) {
                        builder.retryLetterTopic(retryLetterTopic);
                    }
                    if (StringUtils.hasLength(deadLetterTopic)) {
                        builder.deadLetterTopic(deadLetterTopic);
                    }
                    deadLetterPolicy = builder.build();
                }
                consumerBuilder.deadLetterPolicy(deadLetterPolicy);
            }
            // 如果没有设置 ackTimeout,就不进行设置
            if (!Duration.ZERO.equals(globalConsumerProperties.getAckTimeout())) {
                consumerBuilder.ackTimeout(globalConsumerProperties.getAckTimeout().toMillis(), TimeUnit.MILLISECONDS);
            }
            consumerBuilder.messageListener((consumer, msg) -> {
                try {
                    // 执行注册得方法
                    Method handler = consumerHolder.getHandler();
                    Object invoker = consumerHolder.getInvoker();
                    Object args = msg.getValue();
                    if (args instanceof byte[]) {
                        Class<?>[] parameterTypes = handler.getParameterTypes();
                        Class<?> parameterType = parameterTypes[0];
                        if (!parameterType.equals(args.getClass())) {
                            String json = new String(((byte[]) args), StandardCharsets.UTF_8);
                            if (CharSequence.class.isAssignableFrom(parameterType)) {
                                args = json;
                            } else {
                                args = JsonUtil.parseJson(json, parameterType);
                            }
                        }
                    }
                    int parameterCount = handler.getParameterCount();
                    // 如果是一个参数
                    if (parameterCount == 1) {
                        handler.invoke(invoker, args);
                    }
                    // 三个参数的场景
                    if (parameterCount == 3) {
                        handler.invoke(invoker, args, consumer, msg);
                    }
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // 捕获到异常, 取消消费确认(直接投递到重试队列)
                    consumer.negativeAcknowledge(msg);
                    log.error("messageId:{} 的消息消费失败,原因:{}", msg.getMessageId(), e.getMessage(), e);
                }
            });
            Consumer<?> subscribe = consumerBuilder.subscribe();
            log.debug("初始化 topic 为 [{}] 的消费者成功", topic);
            return subscribe;
        } catch (PulsarClientException e) {
            log.error("初始化 topic 为 [{}] 的消费者失败", topic, e);
            throw new InitFailedException("初始化 topic 为 [" + topic + "] 的消费者失败", e);
        }
    }

    public List<Consumer<?>> getConsumers() {
        return consumers;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }
}
