package io.lesible.producer;

import io.lesible.annotation.PulsarProducer;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p> @date: 2021-04-07 16:12</p>
 *
 * @author 何嘉豪
 */
@PulsarProducer
public class ProducerFactory implements IProducerFactory {

    /**
     * topic 和 producer 的映射
     */
    private final Map<String, ProducerHolder> producerHolderMapping = new ConcurrentHashMap<>();

    public ProducerFactory addProducer(String topic, ProducerHolder producerHolder) {
        if (!StringUtils.hasLength(producerHolder.getTopic())) {
            producerHolder.setTopic(topic);
        }
        addProducer(producerHolder);
        return this;
    }

    public ProducerFactory addProducer(ProducerHolder producerHolder) {
        checkTopic(producerHolder);
        producerHolderMapping.put(producerHolder.getTopic(), producerHolder);
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> msgType, String producerName) {
        addProducer(ProducerHolder.builder().topic(topic)
                .msgType(msgType).producerName(producerName).build());
        return this;
    }

    public ProducerFactory addProducer(String topic) {
        addProducer(ProducerHolder.builder().topic(topic).build());
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> msgType) {
        addProducer(ProducerHolder.builder().topic(topic).msgType(msgType).build());
        return this;
    }

    public ProducerFactory addProducer(String topic, String producerName) {
        addProducer(ProducerHolder.builder().topic(topic).producerName(producerName).build());
        return this;
    }

    @Override
    public Map<String, ProducerHolder> getProducersInfo() {
        return producerHolderMapping;
    }

    private void checkTopic(ProducerHolder producerHolder) {
        if (!StringUtils.hasLength(producerHolder.getTopic())) {
            throw new IllegalArgumentException("topic can not be empty");
        }
    }
}
