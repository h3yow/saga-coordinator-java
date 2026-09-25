package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 事务未能正常完成时的可区分原因。
 */
public enum FailureReason {
    NONE,
    STEP_FAILED,
    STEP_TIMEOUT,
    COMPENSATION_FAILED,
    TRANSACTION_TIMEOUT
}
