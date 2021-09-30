package com.sumwhy.pulsar;

import com.sumwhy.pulsar.exception.NoSuchTopicException;
import com.sumwhy.pulsar.model.TopicInfo;
import com.sumwhy.pulsar.producer.collector.ProducerCollector;
import com.sumwhy.pulsar.util.JsonUtil;
import com.sumwhy.pulsar.util.TopicBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * <p> @date: 2021-04-07 17:08</p>
 *
 * @author 何嘉豪
 */
@Slf4j
@Component
@SuppressWarnings("unchecked")
@ConditionalOnProperty(name = "pulsar.enabled", havingValue = "true", matchIfMissing = true)
public class PulsarTemplate {

    private final ProducerCollector producerCollector;

    private final TopicBuilder topicBuilder;

    public PulsarTemplate(ProducerCollector producerCollector, TopicBuilder topicBuilder) {
        this.producerCollector = producerCollector;
        this.topicBuilder = topicBuilder;
    }


    /**
     * 如果是字符串类型, 将直接以 byte 数组形式传送
     *
     * @param topic 主题
     * @param msg   消息
     * @return messageId
     * @throws PulsarClientException 发送时的异常
     */
    public MessageId send(String topic, Object msg) throws PulsarClientException {
        return send(TopicInfo.builder(topic).build(), msg);
    }

    /**
     * 如果是字符串类型, 将直接以 byte 数组形式传送
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @return messageId
     * @throws PulsarClientException 发送时的异常
     */
    public MessageId send(TopicInfo topicInfo, Object msg) throws PulsarClientException {
        return buildMsg(topicInfo, msg).send();
    }

    /**
     * 异步发送消息
     *
     * @param topic 主题
     * @param msg   消息
     * @return CompletableFuture
     */
    public CompletableFuture<MessageId> sendAsync(String topic, Object msg) {
        return sendAsync(TopicInfo.builder(topic).build(), msg);
    }

    /**
     * 异步发送消息
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @return CompletableFuture
     */
    public CompletableFuture<MessageId> sendAsync(TopicInfo topicInfo, Object msg) {
        return buildMsg(topicInfo, msg).sendAsync();
    }

    /**
     * 批量发送, 需要注意的是生产者需要配置允许批量发送,否则只是普通的异步发送
     *
     * @param topic   主题
     * @param msgList 消息列表
     * @return CompletableFuture 集合
     */
    public List<CompletableFuture<MessageId>> batchSend(String topic, List<?> msgList) {
        return batchSend(TopicInfo.builder(topic).build(), msgList);
    }

    /**
     * 批量发送, 需要注意的是生产者需要配置允许批量发送,否则只是普通的异步发送
     *
     * @param topicInfo 主题信息
     * @param msgList   消息列表
     * @return CompletableFuture 集合
     */
    public List<CompletableFuture<MessageId>> batchSend(TopicInfo topicInfo, List<?> msgList) {
        List<CompletableFuture<MessageId>> list = new ArrayList<>();
        for (Object msg : msgList) {
            list.add(buildMsg(topicInfo, msg).sendAsync());
        }
        return list;
    }

    /**
     * 批量发送, 需要注意的是生产者需要配置允许批量发送,否则只是普通的异步发送
     *
     * @param topic 主题
     * @param msg   消息
     * @return CompletableFuture 集合
     */
    public CompletableFuture<MessageId> batchSend(String topic, Object msg) {
        return batchSend(TopicInfo.builder(topic).build(), msg);
    }

    /**
     * 批量发送, 需要注意的是生产者需要配置允许批量发送,否则只是普通的异步发送
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @return CompletableFuture 集合
     */
    public CompletableFuture<MessageId> batchSend(TopicInfo topicInfo, Object msg) {
        return buildMsg(topicInfo, msg).sendAsync();
    }

    /**
     * 批量发送 返回用于检查的对象
     *
     * @param topic   主题
     * @param msgList 消息列表
     * @return CompletableFuture 集合
     */
    public CompletableFuture<Void> batchSend4check(String topic, List<?> msgList) {
        return batchSend4check(TopicInfo.builder(topic).build(), msgList);
    }

    /**
     * 批量发送 返回用于检查的对象
     *
     * @param topicInfo 主题信息
     * @param msgList   消息列表
     * @return CompletableFuture 集合
     */
    public CompletableFuture<Void> batchSend4check(TopicInfo topicInfo, List<?> msgList) {
        List<CompletableFuture<MessageId>> list = batchSend(topicInfo, msgList);
        return CompletableFuture.allOf(list.toArray(new CompletableFuture[0]));
    }

    /**
     * 延时发送信息
     *
     * @param topic     主题
     * @param msg       消息
     * @param afterTime 多久后送达
     * @return messageId
     * @throws PulsarClientException 发送时出现的异常
     */
    public MessageId sendDelayedMessage(String topic, Object msg, Duration afterTime) throws PulsarClientException {
        return sendDelayedMessage(TopicInfo.builder(topic).build(), msg, afterTime);
    }

    /**
     * 延时发送信息
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @param afterTime 多久后送达
     * @return messageId
     * @throws PulsarClientException 发送时出现的异常
     */
    public MessageId sendDelayedMessage(TopicInfo topicInfo, Object msg, Duration afterTime) throws PulsarClientException {
        return buildMsg(topicInfo, msg).deliverAfter(afterTime.getSeconds(), TimeUnit.SECONDS).send();
    }

    /**
     * 异步延时发送信息
     *
     * @param topic     主题
     * @param msg       消息
     * @param afterTime 多久后送达
     * @return messageId
     */
    public CompletableFuture<MessageId> sendDelayedMessageAsync(String topic, Object msg, Duration afterTime) {
        return sendDelayedMessageAsync(TopicInfo.builder(topic).build(), msg, afterTime);
    }

    /**
     * 异步延时发送信息
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @param afterTime 多久后送达
     * @return messageId
     */
    public CompletableFuture<MessageId> sendDelayedMessageAsync(TopicInfo topicInfo, Object msg, Duration afterTime) {
        return buildMsg(topicInfo, msg).deliverAfter(afterTime.getSeconds(), TimeUnit.SECONDS).sendAsync();
    }

    /**
     * 延时发送信息
     *
     * @param topic            主题
     * @param msg              消息
     * @param futureTimeMillis 将来的一个准确的时间戳
     * @return messageId
     * @throws PulsarClientException 发送时出现的异常
     */
    public MessageId sendMessageAtSpecificTime(String topic, Object msg, long futureTimeMillis) throws PulsarClientException {
        return sendMessageAtSpecificTime(TopicInfo.builder(topic).build(), msg, futureTimeMillis);
    }

    /**
     * 延时发送信息
     *
     * @param topicInfo        主题信息
     * @param msg              消息
     * @param futureTimeMillis 将来的一个准确的时间戳
     * @return messageId
     * @throws PulsarClientException 发送时出现的异常
     */
    public MessageId sendMessageAtSpecificTime(TopicInfo topicInfo, Object msg, long futureTimeMillis) throws PulsarClientException {
        return buildMsg(topicInfo, msg).deliverAt(futureTimeMillis).send();
    }

    /**
     * 异步延时发送信息
     *
     * @param topic            主题
     * @param msg              消息
     * @param futureTimeMillis 将来的一个准确的时间戳
     * @return messageId
     */
    public CompletableFuture<MessageId> sendMessageAtSpecificTimeAsync(String topic, Object msg, long futureTimeMillis) {
        return sendMessageAtSpecificTimeAsync(TopicInfo.builder(topic).build(), msg, futureTimeMillis);
    }

    /**
     * 异步延时发送信息
     *
     * @param topicInfo        主题信息
     * @param msg              消息
     * @param futureTimeMillis 将来的一个准确的时间戳
     * @return messageId
     */
    public CompletableFuture<MessageId> sendMessageAtSpecificTimeAsync(TopicInfo topicInfo, Object msg, long futureTimeMillis) {
        return buildMsg(topicInfo, msg).deliverAt(futureTimeMillis).sendAsync();
    }

    private Object easyToSendCharSequence(Object msg) {
        Object actualMsg = msg;
        if (msg instanceof CharSequence) {
            actualMsg = (((CharSequence) msg).toString().getBytes(StandardCharsets.UTF_8));
        } else {
            actualMsg = JsonUtil.jsonValue(msg).getBytes(StandardCharsets.UTF_8);
        }
        return actualMsg;
    }

    /**
     * 构建信息的基础方法
     *
     * @param topicInfo 主题信息
     * @param msg       消息
     * @return 带类型的消息建造者
     */
    @SuppressWarnings("rawtypes")
    private TypedMessageBuilder buildMsg(TopicInfo topicInfo, Object msg) throws NoSuchTopicException {
        Producer producer = producerCollector.getProducer(topicBuilder.buildTopicUrl(topicInfo))
                .orElseThrow(NoSuchTopicException::new);
        return producer.newMessage().value(easyToSendCharSequence(msg));
    }


}
