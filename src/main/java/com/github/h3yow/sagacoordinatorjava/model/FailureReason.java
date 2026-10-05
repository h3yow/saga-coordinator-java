package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 事务未能正常完成时的可区分原因。
 */
public enum FailureReason {
    NONE,
    STEP_FAILED,
    STEP_TIMEOUT,
    COMPENSATION_FAILED,
    TRANSACTION_TIMEOUT,
    /** 已由运维人工处理完毕（对应终态 MANUALLY_RESOLVED）。 */
    MANUALLY_RESOLVED
}
