package com.sumwhy.pulsar.util;

import com.sumwhy.pulsar.model.TopicInfo;
import com.sumwhy.pulsar.properties.PulsarProperties;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * <p> @date: 2021-04-15 14:50</p>
 *
 * @author 何嘉豪
 */
@Service
public class TopicBuilder {

    public static final String DEAD_QUEUE_SUFFIX = "-dlq";
    public static final String RETRY_QUEUE_SUFFIX = "-retry";
    private static final String DEFAULT_PERSISTENCE = "persistent";
    private final PulsarProperties pulsarProperties;

    private TopicBuilder(PulsarProperties pulsarProperties) {
        this.pulsarProperties = pulsarProperties;
    }

    public String buildTopicUrl(String topic) {
        if (checkTopicIfStandard(topic)) {
            return topic;
        }
        return getPrefix() + topic;
    }

    private boolean checkTopicIfStandard(String topic) {
        return topic.contains(DEFAULT_PERSISTENCE + "//") || topic.matches("\\S+/\\S+/\\S+");
    }

    public String buildTopicUrl(String tenant, String namespace, String topic) {
        if (checkTopicIfStandard(topic)) {
            return topic;
        }
        return getPrefix(tenant, namespace) + topic;
    }

    public String buildTopicUrl(TopicInfo topicInfo) {
        String topic = topicInfo.getTopic();
        if (checkTopicIfStandard(topic)) {
            return topic;
        }
        return getPrefix(topicInfo.getTenant(), topicInfo.getNamespace()) + topic;
    }

    public String getPrefix(String tenant, String namespace) {
        return (StringUtils.hasLength(tenant) ? tenant : pulsarProperties.getTenant())
                + "/"
                + (StringUtils.hasLength(namespace) ? namespace : pulsarProperties.getNamespace())
                + "/";
    }

    public String getPrefix() {
        return getPrefix(pulsarProperties.getTenant(), pulsarProperties.getNamespace());
    }

    public String getDeadQueueSuffix() {
        return pulsarProperties.isLowerCase() ? DEAD_QUEUE_SUFFIX : DEAD_QUEUE_SUFFIX.toUpperCase();
    }

    public String getRetryQueueSuffix() {
        return pulsarProperties.isLowerCase() ? RETRY_QUEUE_SUFFIX : RETRY_QUEUE_SUFFIX.toUpperCase();
    }

}
