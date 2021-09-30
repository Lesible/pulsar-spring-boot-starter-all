package com.sumwhy.pulsar.controller;

import com.sumwhy.pulsar.PulsarTemplate;
import com.sumwhy.pulsar.model.DelayWrapper;
import com.sumwhy.pulsar.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * <p> @date: 2021-04-07 14:39</p>
 *
 * @author 何嘉豪
 */
@Slf4j
@RestController
public class TestController {

    @Resource
    private PulsarTemplate pulsarTemplate;

    @PostMapping("/msg")
    public void sendMsg(@RequestBody String msg) throws PulsarClientException {
        //simple-topic
        MessageId msgId = pulsarTemplate.send("simple-topic", msg);
        log.info("messageId:{} 的消息成功发送", msgId);
    }


    @PostMapping("/user")
    public void sendUser(@RequestBody User user) throws Exception {
        //user-topic
        MessageId send = pulsarTemplate.send("user-topic", user);
        log.info("messageId:{} 的消息成功发送", send);
    }

    @PostMapping("/batchSend")
    public void batchSend(@RequestBody ArrayList<User> userList) {
        //user-producer
        CompletableFuture<Void> check = pulsarTemplate.batchSend4check("batch-topic", userList);
        log.info("批量消息成功发送");
    }

    @PostMapping("/delayAt")
    private void delayAt(@RequestBody DelayWrapper delayWrapper) throws Exception {
        log.info("receive delayAt msg at time {}",
                DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()));
        MessageId messageId = pulsarTemplate.sendMessageAtSpecificTime("delay-at-topic",
                delayWrapper.getUser(), delayWrapper.getDelayAt());
        log.info("messageId:{} 的消息成功发送", messageId);
    }

    @PostMapping("/delayAfter")
    private void delayAfter(@RequestBody DelayWrapper delayWrapper) throws Exception {
        log.info("receive delayAfter msg at time {}",
                DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()));
        MessageId messageId = pulsarTemplate.sendDelayedMessage("delay-after-topic",
                delayWrapper.getUser(), Duration.ofMillis(delayWrapper.getDelayAfter()));
        log.info("messageId:{} 的消息成功发送", messageId);
    }
}
