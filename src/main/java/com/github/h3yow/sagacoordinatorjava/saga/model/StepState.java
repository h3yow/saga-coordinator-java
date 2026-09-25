package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 单个参与者步骤的状态。
 */
public enum StepState {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED
}
