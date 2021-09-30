package com.sumwhy.pulsar.model;

import lombok.Data;

/**
 * <p> @date: 2021-04-07 18:10</p>
 *
 * @author 何嘉豪
 */
@Data
public class DelayWrapper {

    private long delayAfter;

    private long delayAt;

    private User user;

}
