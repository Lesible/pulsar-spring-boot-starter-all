package com.sumwhy.pulsar.producer;

import com.sumwhy.pulsar.model.ProducerHolder;
import com.sumwhy.pulsar.model.TopicInfo;

import java.util.Map;

/**
 * <p> @date: 2021-04-07 15:57</p>
 *
 * @author 何嘉豪
 */
public interface IProducerFactory {

    /**
     * 获取 topic 和 producer 信息的映射
     *
     * @return topic 和 producer 信息的映射
     */
    Map<TopicInfo, ProducerHolder> getProducersInfo();

}
