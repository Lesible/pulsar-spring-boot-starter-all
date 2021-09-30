package com.sumwhy.pulsar.model;

import lombok.Getter;
import lombok.Setter;

/**
 * <p> topic 信息  </p>
 * <p> create 2021-08-27 16:29 by lesible </p>
 *
 * @author 何嘉豪
 */
@Getter
@Setter
public class TopicInfo {

    /**
     * 租户名
     */
    private final String tenant;

    /**
     * 命名空间
     */
    private final String namespace;

    /**
     * 主题名
     */
    private final String topic;

    private TopicInfo(Builder builder) {
        this.tenant = builder.tenant;
        this.namespace = builder.namespace;
        this.topic = builder.topic;
    }

    public static Builder builder(String topic) {
        return new Builder(topic);
    }

    public static class Builder {

        private final String topic;
        private String tenant;
        private String namespace;

        private Builder(String topic) {
            this.topic = topic;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public TopicInfo build() {
            return new TopicInfo(this);
        }

    }


}
