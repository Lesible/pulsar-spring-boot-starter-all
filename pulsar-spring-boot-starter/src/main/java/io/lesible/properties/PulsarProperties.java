package io.lesible.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * <p> @date: 2021-04-06 15:52</p>
 *
 * @author 何嘉豪
 */
@Data
@ConfigurationProperties(prefix = "pulsar")
public class PulsarProperties {

    /**
     * serviceUrl
     * <p>
     * 可以是集群列表 domain1:port1,domain2:port2
     * Configure the service URL for the Pulsar service.
     * This parameter is required.
     * Examples:
     * pulsar://my-broker:6650 for regular endpoint
     * pulsar+ssl://my-broker:6651 for TLS encrypted endpoint
     */
    private String serviceUrl;

    /**
     * 开关,是否为腾讯云 tdmq,根据类型生成不同的 client 对象
     */
    private boolean isTdmq;

    /**
     * 腾讯云的路由 id
     */
    private String routeId;

    /**
     * 腾讯云的角色密钥
     */
    private String token;

    /**
     * 处理 broker 和 client 之间连接用的 io 线程
     * the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>.
     */
    private Integer ioThreads = 10;

    /**
     * 设置使用 listener 模式的 reader 和 consumer 处理的线程数
     * the number of threads to be used for message listeners <i>(default: 1 thread)</i>.
     *
     * <p>The listener thread pool is shared across all the consumers and readers that are
     * using a "listener" model to get messages. For a given consumer, the listener will be
     * always invoked from the same thread, to ensure ordering.
     */
    private Integer listenerThreads = 10;

    /**
     * 每个 broker 最大的连接数
     * By default, the connection pool will use a single connection for all the producers and consumers.
     * Increasing this parameter may improve throughput when using many producers over a high latency connection.
     */
    private int connectionsPerBroker = 1;


    /**
     * client 和 broker 连接空闲后保持存活的时间 默认 30s
     * <p>
     * Set keep alive interval for each client-broker-connection. <i>(default: 30 seconds)</i>.
     */
    private Duration keepAliveInterval = Duration.ofSeconds(30);

    /**
     * 连接超时时间, 默认 10s
     * <p>
     * Set the duration of time to wait for a connection to a broker to be established.
     * <p>
     * If the duration passes without a response from the broker, the connection attempt is dropped.
     */
    private Duration connectionTimeout = Duration.ofSeconds(10);

    /**
     * 操作超时时间, 默认 30s
     * <p>
     * Set the operation timeout (default: 30 seconds).
     * Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the operation will be marked as failed
     */
    private Duration operationTimeout = Duration.ofSeconds(30);

    /**
     * 开始重试补偿的时间, 默认 100ms
     * <p>
     * Set the duration of time for a backoff interval.
     */
    private Duration startingBackoffInterval = Duration.ofMillis(100);

    /**
     * 重试补偿的间隔时间,默认 60s
     * <p>
     * Set the maximum duration of time for a backoff interval.
     */
    private Duration maxBackoffInterval = Duration.ofSeconds(60);

    /**
     * 命名空间
     */
    private String namespace = "default";

    /**
     * (租户)/(tdmq 集群 id)
     */
    private String tenant = "public";

    /**
     * 是否启用 pulsar
     */
    private boolean enabled;

    /**
     * 队列后缀使用大小写
     */
    private boolean lowerCase = true;

}