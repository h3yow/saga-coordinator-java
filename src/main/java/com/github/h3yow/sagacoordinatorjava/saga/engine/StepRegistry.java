package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按步骤名注册参与者实现；崩溃恢复后用它重新解析步骤。
 */
public class StepRegistry {

    private final Map<String, SagaStep> steps = new ConcurrentHashMap<>();

    public void register(SagaStep step) {
        steps.put(step.name(), step);
    }

    public Optional<SagaStep> find(String name) {
        return Optional.ofNullable(steps.get(name));
    }
}
