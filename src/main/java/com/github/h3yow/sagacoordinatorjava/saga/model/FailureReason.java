package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 事务未能正常收敛时可区分的原因。
 */
public enum FailureReason {
    NONE,
    STEP_FAILED,
    STEP_TIMEOUT,
    COMPENSATION_FAILED,
    SAGA_TIMEOUT,
    STEP_UNKNOWN,
    ILLEGAL_TRANSITION
}
