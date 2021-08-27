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
     * 租户
     */
    private String tenant;

    /**
     * 命名空间
     */
    private String namespace;

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

    private ProducerHolder(Builder builder) {
        this.topic = builder.topic;
        this.msgType = builder.msgType;
        this.producerName = builder.producerName;
        this.tenant = builder.tenant;
        this.namespace = builder.namespace;
        this.sendTimeout = builder.sendTimeout;
        this.blockIfQueueFull = builder.blockIfQueueFull;
        this.enableBatching = builder.enableBatching;
        this.batchingMaxPublishDelay = builder.batchingMaxPublishDelay;
        this.batchingMaxMessages = builder.batchingMaxMessages;
        this.batchingMaxBytes = builder.batchingMaxBytes;
    }

    public static Builder builder(String topic) {
        return new Builder(topic);
    }

    /**
     * builder
     */
    public static class Builder {

        private final String topic;

        private Class<?> msgType = byte[].class;

        private String producerName = "";

        private String tenant = "";

        private String namespace = "";

        private Duration sendTimeout = Duration.ofSeconds(30);

        private boolean blockIfQueueFull;

        private boolean enableBatching;

        private Duration batchingMaxPublishDelay = Duration.ofMillis(1L);

        private int batchingMaxMessages = 1000;

        private int batchingMaxBytes = 1 << 17;

        private Builder(String topic) {
            this.topic = topic;
        }

        public Builder msgType(Class<?> msgType) {
            this.msgType = msgType;
            return this;
        }

        public Builder producerName(String producerName) {
            this.producerName = producerName;
            return this;
        }

        public Builder sendTimeout(Duration sendTimeout) {
            this.sendTimeout = sendTimeout;
            return this;
        }

        public Builder blockIfQueueFull(boolean blockIfQueueFull) {
            this.blockIfQueueFull = blockIfQueueFull;
            return this;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder enableBatching(boolean enableBatching) {
            this.enableBatching = enableBatching;
            return this;
        }

        public Builder batchingMaxPublishDelay(Duration batchingMaxPublishDelay) {
            this.batchingMaxPublishDelay = batchingMaxPublishDelay;
            return this;
        }

        public Builder batchingMaxMessages(int batchingMaxMessages) {
            this.batchingMaxMessages = batchingMaxMessages;
            return this;
        }

        public Builder batchingMaxBytes(int batchingMaxBytes) {
            this.batchingMaxBytes = batchingMaxBytes;
            return this;
        }

        public ProducerHolder build() {
            return new ProducerHolder(this);
        }
    }
}
