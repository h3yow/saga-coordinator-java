package com.github.h3yow.sagacoordinatorjava.saga.engine;

import java.util.Collection;
import java.util.Optional;

/**
 * 事务实例的内存索引（由事件日志重放构建）。
 */
public interface SagaRepository {

    void save(SagaInstance instance);

    Optional<SagaInstance> find(String sagaId);

    Collection<SagaInstance> findAll();
}
