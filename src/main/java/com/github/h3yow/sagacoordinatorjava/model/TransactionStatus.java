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
 *
 * COMPENSATION_FAILED ──> COMPENSATING（人工再试一次补偿）
 * COMPENSATION_FAILED ──> MANUALLY_RESOLVED（人工确认处理完毕，终态）
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
    /** 运维人工确认：事务残留效果已由人工处理完毕，终态。 */
    MANUALLY_RESOLVED
}
