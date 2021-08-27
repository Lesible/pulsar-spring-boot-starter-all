package io.lesible.util;

import io.lesible.properties.PulsarProperties;
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
        return getPrefix() + topic;
    }

    public String buildTopicUrl(String tenant, String namespace, String topic) {
        return getPrefix(tenant, namespace) + topic;
    }

    public String getPrefix(String tenant, String namespace) {
        return DEFAULT_PERSISTENCE + "://"
                + (StringUtils.hasLength(tenant) ? tenant : pulsarProperties.getTenant())
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
