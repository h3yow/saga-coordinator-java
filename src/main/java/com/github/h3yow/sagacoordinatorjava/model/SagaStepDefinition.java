package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 一个参与者步骤的定义：正向动作与对应补偿动作。
 *
 * @param <T> 步骤间传递的上下文类型
 */
public class SagaStepDefinition<T> {

    private String name;
    private SagaAction<T> action;
    private SagaAction<T> compensation;

    public SagaStepDefinition() {
    }

    public SagaStepDefinition(String name, SagaAction<T> action, SagaAction<T> compensation) {
        this.name = name;
        this.action = action;
        this.compensation = compensation;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SagaAction<T> getAction() {
        return action;
    }

    public void setAction(SagaAction<T> action) {
        this.action = action;
    }

    public SagaAction<T> getCompensation() {
        return compensation;
    }

    public void setCompensation(SagaAction<T> compensation) {
        this.compensation = compensation;
    }
}
