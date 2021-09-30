package com.sumwhy.pulsar.util;

import org.apache.pulsar.client.api.Schema;

/**
 * <p> @date: 2021-04-07 16:37</p>
 *
 * @author 何嘉豪
 */
public class SchemaUtil {

    public static Schema<?> schema(Class<?> clazz) {
        Schema<?> schema;
        if (clazz == null || clazz.equals(byte[].class)) {
            schema = Schema.BYTES;
        } else {
            schema = Schema.JSON(clazz);
        }
        return schema;
    }

}
