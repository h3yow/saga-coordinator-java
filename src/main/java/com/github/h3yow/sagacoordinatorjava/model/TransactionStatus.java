package com.github.h3yow.sagacoordinatorjava.model;

/**
 * Saga 事务状态机。
 *
 * <pre>
 * PENDING ──> RUNNING ──> COMPLETED
 *                │
 *                ├──> COMPENSATING ──> COMPENSATED
 *                │         └────────> COMPENSATION_FAILED ──> COMPENSATING (人工重试补偿)
 *                │                            └───────────> MANUALLY_RESOLVED (人工标记完结)
 *                ├──> COMPENSATED      (步骤尚未执行即失败的快速回滚路径)
 *                ├──> FAILED
 *                └──> TIMEOUT_COMPENSATING ──> TIMED_OUT_COMPENSATED
 * </pre>
 */
public enum TransactionStatus {
    PENDING,
    RUNNING,
    COMPENSATING,
    COMPENSATED,
    COMPLETED,
    FAILED,
    COMPENSATION_FAILED,
    TIMEOUT_COMPENSATING,
    TIMED_OUT_COMPENSATED,
    /** 人工介入后显式标记完结（终态） */
    MANUALLY_RESOLVED
}
