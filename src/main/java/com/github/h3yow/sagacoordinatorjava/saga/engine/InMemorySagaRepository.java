package com.github.h3yow.sagacoordinatorjava.saga.engine;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 简单内存仓储。真实事实来源是 {@link com.github.h3yow.sagacoordinatorjava.saga.store.EventLog}。
 */
public class InMemorySagaRepository implements SagaRepository {

    private final Map<String, SagaInstance> store = new ConcurrentHashMap<>();

    @Override
    public void save(SagaInstance instance) {
        store.put(instance.getSagaId(), instance);
    }

    @Override
    public Optional<SagaInstance> find(String sagaId) {
        return Optional.ofNullable(store.get(sagaId));
    }

    @Override
    public Collection<SagaInstance> findAll() {
        return store.values();
    }
}
