package io.lesible.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.ReferenceType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * <p> @date: 2021-06-08 20:11</p>
 *
 * @author 何嘉豪
 */
@Slf4j
public class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String jsonValue(Object o, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            log.error("转换 json 出错", e);
            return "{}";
        }
    }

    public static String jsonValue(Object o) {
        return jsonValue(o, OBJECT_MAPPER);
    }

    public static <T> T parseJson(String json, ObjectMapper objectMapper, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            log.error("解析 json 出错 ", e);
            return null;
        }
    }

    public static <T> T parseJson(String json, ObjectMapper objectMapper, JavaType javaType) {
        try {
            return objectMapper.readValue(json, javaType);
        } catch (JsonProcessingException e) {
            log.error("解析 json 出错 ", e);
            return null;
        }
    }

    public static <T> T parseJson(String json, ObjectMapper objectMapper, ReferenceType referenceType) {
        try {
            return objectMapper.readValue(json, referenceType);
        } catch (JsonProcessingException e) {
            log.error("解析 json 出错 ", e);
            return null;
        }
    }

    public static <T> T parseJson(String json, Class<T> clazz) {
        return parseJson(json, OBJECT_MAPPER, clazz);
    }

    public static <T> T parseJson(String json, JavaType javaType) {
        return parseJson(json, OBJECT_MAPPER, javaType);
    }

    public static <T> T parseJson(String json, ReferenceType referenceType) {
        return parseJson(json, OBJECT_MAPPER, referenceType);
    }

    public static <K, V> Map<K, V> parseMap(String json, ObjectMapper objectMapper, Class<K> kClazz, Class<V> vClazz) {
        TypeFactory typeFactory = objectMapper.getTypeFactory();
        MapType mapType = typeFactory.constructMapType(HashMap.class, kClazz, vClazz);
        return parseJson(json, objectMapper, mapType);
    }

    public static <K, V> Map<K, V> parseMap(String json, Class<K> kClazz, Class<V> vClazz) {
        return parseMap(json, OBJECT_MAPPER, kClazz, vClazz);
    }

}
