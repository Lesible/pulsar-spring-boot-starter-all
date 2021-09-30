package com.sumwhy.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sumwhy.pulsar.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * <p> @date: 2021-04-08 13:45</p>
 *
 * @author 何嘉豪
 */
@Slf4j
public class JsonPrint {

    @Test
    public void print() throws Exception {
        List<User> users = new ArrayList<>();
        users.add(new User(123L, "Relic"));
        users.add(new User(234L, "Lesible"));
        ObjectMapper om = new ObjectMapper();
        String json = om.writeValueAsString(users);
        log.info("json: {}", json);
    }

    @Test
    public void currentTimeMillis() {
        long currentTimeMillis = System.currentTimeMillis();
        log.info("30s after: {}", currentTimeMillis + 30000);
    }

    @Test
    public void produceMsg() throws Exception {
        PulsarClient build = PulsarClient.builder().serviceUrl("pulsar://192.168.110.200:6650").build();
        Producer<byte[]> producer = build.newProducer().topic("persistent://public/default/simple-topic").create();
        producer.send("this is relic's test".getBytes(StandardCharsets.UTF_8));
    }

}
