package io.lesible.annotation;

import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p> @date: 2021-04-06 16:53</p>
 *
 * @author 何嘉豪
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PulsarConsumer {

    /**
     * 设置需要消费的主题,允许使用 spEL
     *
     * @return 消费主题
     */
    @AliasFor(value = "value")
    String topic() default "";

    /**
     * 默认方法, 与 topic 映射
     *
     * @return topic
     */
    @AliasFor(value = "topic")
    String value() default "";

    /**
     * 发送消息的类型 默认为 byte[]
     *
     * @return 发送消息的类型
     */
    Class<?> msgType() default byte[].class;

    /**
     * 订阅的类型, 默认为 Exclusive 此处默认为 Shared,便于使用延时队列
     *
     * @return 订阅类型
     */
    SubscriptionType subscriptionType() default SubscriptionType.Shared;

    /**
     * 订阅的名称
     *
     * @return 订阅名称
     */
    String subscriptionName() default "";

    /**
     * 是否开启重试
     *
     * @return 默认是
     */
    boolean retryEnable() default true;

    /**
     * 重试,死信队列配置,只有重试开启才有效
     *
     * @return 重试, 死信队列配置
     */
    DeadLetter deadLetter() default @DeadLetter;

    /**
     * 消费者的名称,需要保证全局唯一
     *
     * @return 消费者名称
     */
    String consumerName() default "";

    /**
     * 租户名称
     *
     * @return 租户名称
     */
    String tenant() default "";

    /**
     * 命名空间
     *
     * @return 命名空间
     */
    String namespace() default "";

    /**
     * 设置预拉取消息的最大大小,默认为 1000,提高大小会增加吞吐量,同时影响批量消息,但是提高内存使用.
     *
     * @return 接收队列的大小
     */
    int receiverQueueSize() default 1000;

}
