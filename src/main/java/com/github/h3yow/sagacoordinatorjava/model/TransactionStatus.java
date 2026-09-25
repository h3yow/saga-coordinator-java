package com.github.h3yow.sagacoordinatorjava.model;

/**
 * Saga 事务状态机。
 *
 * <pre>
 * PENDING ──> RUNNING ──> COMPLETED
 *                │
 *                ├──> COMPENSATING ──> COMPENSATED
 *                │         └────────> COMPENSATION_FAILED (终态，待人工处理)
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
    TIMED_OUT_COMPENSATED
}
