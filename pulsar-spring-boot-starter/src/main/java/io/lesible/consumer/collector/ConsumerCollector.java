package io.lesible.consumer.collector;

import io.lesible.annotation.PulsarConsumer;
import io.lesible.consumer.ConsumerHolder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * <p> @date: 2021-04-07 10:18</p>
 * <p>
 * consumer 收集器, 当 bean 实例构造完成之后将所有 consumer 手机到本实例
 *
 * @author 何嘉豪
 */
@Slf4j
@Configuration
public class ConsumerCollector implements BeanPostProcessor {

    /**
     * 保存 consumer 名称和 consumerHolder 的映射
     */
    private final Map<String, ConsumerHolder> consumerHolderMapping = new ConcurrentHashMap<>(16);

    /**
     * 为了防止有人设置重复的 consumerName~
     */
    private final AtomicInteger index = new AtomicInteger();

    /**
     * 初始化 consumer 映射
     *
     * @param bean     所有受 spring 容器管理的 bean
     * @param beanName bean 的名称
     * @return bean
     * @throws BeansException beansException
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();
        consumerHolderMapping.putAll(Arrays.stream(beanClass.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(PulsarConsumer.class))
                .map(method -> new ConsumerHolder(AnnotationUtils.getAnnotation(method, PulsarConsumer.class), bean, method))
                .collect(Collectors.toMap(this::getConsumerName, consumer -> consumer,
                        // 为旧值添加下标
                        (oldValue, newValue) -> {
                            consumerHolderMapping.put(getConsumerName(oldValue) + "_" + index.incrementAndGet(), oldValue);
                            return newValue;
                        })));
        return bean;
    }

    /**
     * 如果配置了 consumerName,直接返回 consumerName ,否则根据配置类名称,方法名,参数列表来生成 consumerName
     *
     * @param consumer consumerHolder
     * @return 全局唯一 consumer
     */
    private String getConsumerName(ConsumerHolder consumer) {
        Class<?> aClass = consumer.getInvoker().getClass();
        String consumerName = consumer.getPulsarConsumer().consumerName();
        if (!StringUtils.hasLength(consumerName)) {
            Method handler = consumer.getHandler();
            consumerName = aClass.getName() + "." + handler.getName() + "(" +
                    Arrays.stream(handler.getGenericParameterTypes())
                            .map(Type::getTypeName).collect(Collectors.joining(",")) + ")";
        }
        return consumerName;
    }

    /**
     * 获取 consumerHolder 映射
     *
     * @return consumerName 和 consumerHolder 的映射关系
     */
    public Map<String, ConsumerHolder> getConsumerHolderMapping() {
        return consumerHolderMapping;
    }

    /**
     * 根据 consumerName 获取指定的 consumerHolder
     *
     * @param consumerName 消费者名称
     * @return consumerHolder
     */
    public Optional<ConsumerHolder> getConsumerHolder(String consumerName) {
        return Optional.ofNullable(consumerHolderMapping.get(consumerName));
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        List<ConsumerHolder> methods = new ArrayList<>();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            List<PulsarConsumer> annotations = MergedAnnotations.from(method, MergedAnnotations.SearchStrategy.TYPE_HIERARCHY)
                    .stream(PulsarConsumer.class)
                    .map(MergedAnnotation::synthesize)
                    .collect(Collectors.toList());
            if (annotations.size() > 0) {
                methods.add(new ConsumerHolder(annotations.get(0), bean, method));
            }
        }, ReflectionUtils.USER_DECLARED_METHODS);
        if (!CollectionUtils.isEmpty(methods)) {
            log.info("methods: {}", methods);
        }
        return bean;
    }

}
