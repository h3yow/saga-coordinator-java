package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 参与者步骤。实现方必须保证 {@link #execute(SagaContext)} 与
 * {@link #compensate(SagaContext)} 自身幂等：协调器在崩溃恢复/超时恢复时
 * 可能以相同的 operationId 重复调用。
 */
public interface SagaStep {

    /** 步骤唯一名（用于编排与持久化后重放解析）。 */
    String name();

    /** 正向提交。 */
    StepOutcome execute(SagaContext context) throws Exception;

    /** 逆序补偿。 */
    StepOutcome compensate(SagaContext context) throws Exception;
}
