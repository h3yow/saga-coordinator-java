package com.github.h3yow.sagacoordinatorjava.saga;

import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 已注册的 Saga 编排定义。定义本身不随事务持久化，
 * 恢复时通过 sagaType 重新取出定义继续推进。
 *
 * @param <T> 上下文类型
 */
public class SagaRegistry {

    public record Registration<T>(SagaDefinition<T> definition, Supplier<T> contextFactory) {
    }

    private final Map<String, Registration<?>> registrations = new ConcurrentHashMap<>();

    public <T> void register(SagaDefinition<T> definition, Supplier<T> contextFactory) {
        registrations.put(definition.getType(), new Registration<>(definition, contextFactory));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<Registration<T>> get(String type) {
        return Optional.ofNullable((Registration<T>) registrations.get(type));
    }
}
