package io.lesible.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * <p> @date: 2021-04-06 15:51</p>
 *
 * @author 何嘉豪
 */
@Data
@ConfigurationProperties(prefix = "pulsar.consumer.default")
public class GlobalConsumerProperties {

    /**
     * 消费确认超时时间,必须大于一秒
     */
    private Duration ackTimeout = Duration.ZERO;


}
