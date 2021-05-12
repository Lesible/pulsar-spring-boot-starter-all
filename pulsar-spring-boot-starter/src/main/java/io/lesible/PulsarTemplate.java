package io.lesible;

import io.lesible.exception.NoSuchTopicException;
import io.lesible.producer.collector.ProducerCollector;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ProducerCollector producerCollector;

    public PulsarTemplate(ProducerCollector producerCollector) {
        this.producerCollector = producerCollector;
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
        return buildMsg(topic, msg).send();
    }

    /**
     * 异步发送消息
     *
     * @param topic 主题
     * @param msg   消息
     * @return CompletableFuture
     */
    public CompletableFuture<MessageId> sendAsync(String topic, Object msg) {
        return buildMsg(topic, msg).sendAsync();
    }

    /**
     * 批量发送, 需要注意的是生产者需要配置允许批量发送,否则只是普通的异步发送
     *
     * @param topic   主题
     * @param msgList 消息列表
     * @return CompletableFuture 集合
     */
    public List<CompletableFuture<MessageId>> batchSend(String topic, List<?> msgList) {
        List<CompletableFuture<MessageId>> list = new ArrayList<>();
        for (Object msg : msgList) {
            list.add(buildMsg(topic, msg).sendAsync());
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
        return buildMsg(topic, msg).sendAsync();
    }

    /**
     * 批量发送 返回用于检查的对象
     *
     * @param topic   主题
     * @param msgList 消息列表
     * @return CompletableFuture 集合
     */
    public CompletableFuture<Void> batchSend4check(String topic, List<?> msgList) {
        List<CompletableFuture<MessageId>> list = batchSend(topic, msgList);
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
        return buildMsg(topic, msg).deliverAfter(afterTime.getSeconds(), TimeUnit.SECONDS).send();
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
        return buildMsg(topic, msg).deliverAfter(afterTime.getSeconds(), TimeUnit.SECONDS).sendAsync();
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
        return buildMsg(topic, msg).deliverAt(futureTimeMillis).send();
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
        return buildMsg(topic, msg).deliverAt(futureTimeMillis).sendAsync();
    }

    private Object easyToSendCharSequence(Object msg) {
        Object actualMsg = msg;
        if (msg instanceof CharSequence) {
            actualMsg = (((CharSequence) msg).toString().getBytes(StandardCharsets.UTF_8));
        } else {
            try {
                actualMsg = OBJECT_MAPPER.writeValueAsString(msg).getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                log.error("序列化实体类失败", e);
            }
        }
        return actualMsg;
    }

    /**
     * 构建信息的基础方法
     *
     * @param topic 主题
     * @param msg   消息
     * @return 带类型的消息建造者
     */
    @SuppressWarnings("rawtypes")
    private TypedMessageBuilder buildMsg(String topic, Object msg) throws NoSuchTopicException {
        Producer producer = producerCollector.getProducer(topic)
                .orElseThrow(NoSuchTopicException::new);
        return producer.newMessage().value(easyToSendCharSequence(msg));
    }

}
