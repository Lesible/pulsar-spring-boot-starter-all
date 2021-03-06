package com.sumwhy.pulsar.producer;

import com.sumwhy.pulsar.annotation.PulsarProducer;
import com.sumwhy.pulsar.model.ProducerHolder;
import com.sumwhy.pulsar.model.TopicInfo;
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
    private final Map<TopicInfo, ProducerHolder> producerHolderMapping = new ConcurrentHashMap<>();

    public ProducerFactory addProducer(String topic, ProducerHolder producerHolder) {
        if (!StringUtils.hasLength(producerHolder.getTopic())) {
            producerHolder.setTopic(topic);
        }
        addProducer(producerHolder);
        return this;
    }

    public ProducerFactory addProducer(ProducerHolder producerHolder) {
        checkTopic(producerHolder);
        producerHolderMapping.put(TopicInfo.builder(producerHolder.getTopic())
                .namespace(producerHolder.getNamespace())
                .tenant(producerHolder.getTenant()).build(), producerHolder);
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> msgType, String producerName) {
        addProducer(ProducerHolder.builder(topic)
                .msgType(msgType).producerName(producerName).build());
        return this;
    }

    public ProducerFactory addProducer(String topic) {
        addProducer(ProducerHolder.builder(topic).build());
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> msgType) {
        addProducer(ProducerHolder.builder(topic).msgType(msgType).build());
        return this;
    }

    public ProducerFactory addProducer(String topic, String producerName) {
        addProducer(ProducerHolder.builder(topic).producerName(producerName).build());
        return this;
    }

    @Override
    public Map<TopicInfo, ProducerHolder> getProducersInfo() {
        return producerHolderMapping;
    }

    private void checkTopic(ProducerHolder producerHolder) {
        if (!StringUtils.hasLength(producerHolder.getTopic())) {
            throw new IllegalArgumentException("topic can not be empty");
        }
    }
}
