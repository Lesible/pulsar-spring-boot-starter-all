package io.lesible.producer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Duration;

/**
 * <p> @date: 2021-04-07 15:58</p>
 *
 * @author 何嘉豪
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerHolder {

    /**
     * 生产者的 topic
     */
    private String topic;

    /**
     * 消息格式
     */
    private Class<?> msgType = byte[].class;

    /**
     * producer 的名称
     */
    private String producerName;

    /**
     * 发送超时时间,默认为 30 秒, 设置为 0,则不限时
     * <p>
     * 如果超时时间内没有成功被服务端确认,将会抛出一个异常
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * <p>Setting the timeout to zero, for example {@code setTimeout(0, TimeUnit.SECONDS)} will set the timeout
     * to infinity, which can be useful when using Pulsar's message deduplication feature, since the client
     * library will retry forever to publish a message. No errors will be propagated back to the application.
     */
    private Duration sendTimeout;


    /**
     * 控制当队列满时是阻塞线程还是直接抛出异常
     */
    private boolean blockIfQueueFull;

    /**
     * 是否使用批量发送
     */
    private boolean enableBatching;

    /**
     * 设置批量发送消息的最大延时 (<i>default: 1 ms</i>),如果设置为一个非 0 的数值,满足以下条件才会发送.
     * <p>
     * 1.超过延时时间.
     * <p>
     * 2.消息数量达到了一批中允许的最大值 (count).
     * <p>
     * 3.消息大小达到了一批中允许的最大值 (size,单位为 B)
     * <p>
     * All messages will be published as a single batch message. The consumer will be delivered individual
     * messages in the batch in the same order they were enqueued.
     */
    private Duration batchingMaxPublishDelay;

    /**
     * 一批消息中允许最大的消息数量 (count <i>default: 1000</i>)
     */
    private int batchingMaxMessages = 1000;

    /**
     * 一批消息中允许最大的消息大小 (size,单位为 B <i>default: 128KB</i>)
     */
    private int batchingMaxBytes = 1 << 17;

    public static ProducerHolderBuilder builder() {
        return new ProducerHolderBuilder();
    }

    /**
     * builder
     */
    public static class ProducerHolderBuilder {

        private final ProducerHolder producerHolder = new ProducerHolder();

        public ProducerHolderBuilder() {
        }

        public ProducerHolder.ProducerHolderBuilder topic(String topic) {
            producerHolder.setTopic(topic);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder msgType(Class<?> msgType) {
            producerHolder.setMsgType(msgType);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder producerName(String producerName) {
            producerHolder.setProducerName(producerName);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder sendTimeout(Duration sendTimeout) {
            producerHolder.setSendTimeout(sendTimeout);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder blockIfQueueFull(boolean blockIfQueueFull) {
            producerHolder.setBlockIfQueueFull(blockIfQueueFull);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder enableBatching(boolean enableBatching) {
            producerHolder.setEnableBatching(enableBatching);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder batchingMaxPublishDelay(Duration batchingMaxPublishDelay) {
            producerHolder.setBatchingMaxPublishDelay(batchingMaxPublishDelay);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder batchingMaxMessages(int batchingMaxMessages) {
            producerHolder.setBatchingMaxMessages(batchingMaxMessages);
            return this;
        }

        public ProducerHolder.ProducerHolderBuilder batchingMaxBytes(int batchingMaxBytes) {
            producerHolder.setBatchingMaxBytes(batchingMaxBytes);
            return this;
        }

        public ProducerHolder build() {
            return producerHolder;
        }
    }
}
