package com.github.h3yow.sagacoordinatorjava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Saga 编排定义：有序的参与者步骤列表。
 */
public class SagaDefinition<T> {

    private String type;
    private List<SagaStepDefinition<T>> steps = new ArrayList<>();

    public SagaDefinition() {
    }

    public SagaDefinition(String type) {
        this.type = type;
    }

    public SagaDefinition<T> step(String name, SagaAction<T> action, SagaAction<T> compensation) {
        steps.add(new SagaStepDefinition<>(name, action, compensation));
        return this;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<SagaStepDefinition<T>> getSteps() {
        return steps;
    }

    public void setSteps(List<SagaStepDefinition<T>> steps) {
        this.steps = steps;
    }
}
