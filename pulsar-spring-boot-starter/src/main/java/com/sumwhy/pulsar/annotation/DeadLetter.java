package com.sumwhy.pulsar.annotation;

import org.apache.pulsar.client.util.RetryMessageUtil;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p> @date: 2021-04-07 14:17</p>
 *
 * <p> 死信配置 </p>
 *
 * @author 何嘉豪
 */
@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DeadLetter {

    /**
     * 默认重新投递次数 16次
     *
     * @return 重新投递的次数
     */
    int maxRedeliverCount() default RetryMessageUtil.MAX_RECONSUMETIMES;

    /**
     * 重试队列 topic 名称
     *
     * @return 重试队列
     */
    String retryLetterTopic() default "";

    /**
     * 死信队列 topic 名称
     *
     * @return 死信队列
     */
    String deadLetterTopic() default "";

}
