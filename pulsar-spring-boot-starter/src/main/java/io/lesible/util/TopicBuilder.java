package io.lesible.util;

import io.lesible.properties.PulsarProperties;
import org.springframework.stereotype.Service;

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
        return DEFAULT_PERSISTENCE + "://" + pulsarProperties.getTenant() + "/" + pulsarProperties.getNamespace() +
                "/" + topic;
    }

    public String getPrefix() {
        return DEFAULT_PERSISTENCE + "://" + pulsarProperties.getTenant() + "/" + pulsarProperties.getNamespace() +
                "/";
    }

}
