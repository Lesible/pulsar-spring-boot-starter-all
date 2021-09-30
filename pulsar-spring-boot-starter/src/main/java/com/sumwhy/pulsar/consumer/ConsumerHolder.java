package com.sumwhy.pulsar.consumer;

import com.sumwhy.pulsar.annotation.PulsarConsumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Method;

/**
 * <p> @date: 2021-04-07 10:02</p>
 *
 * @author 何嘉豪
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerHolder {

    private PulsarConsumer pulsarConsumer;

    private Object invoker;

    private Method handler;

}
