package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 事务（Saga）全局状态。
 */
public enum SagaState {
    STARTED,
    RUNNING,
    COMPENSATING,
    COMPLETED,
    COMPENSATED,
    COMPENSATION_FAILED,
    FAILED,
    TIMED_OUT,
    MANUAL_INTERVENTION
}
