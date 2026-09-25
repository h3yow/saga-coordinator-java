package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 参与者动作（正向提交或补偿）。实现必须保证幂等：
 * 协调器在超时重试 / 崩溃恢复时可能对同一步骤重复调用。
 *
 * @param <T> 上下文类型
 */
@FunctionalInterface
public interface SagaAction<T> {
    void execute(T context) throws Exception;
}
