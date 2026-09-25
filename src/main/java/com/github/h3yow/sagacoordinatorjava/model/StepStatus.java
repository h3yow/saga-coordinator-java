package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 单个参与者步骤的状态机。
 *
 * <pre>
 * INIT ──> EXECUTING ──> COMPLETED ──> COMPENSATING ──> COMPENSATED
 *                  └────> FAILED
 *                  └────> TIMED_OUT
 * </pre>
 */
public enum StepStatus {
    INIT,
    EXECUTING,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED,
    TIMED_OUT
}
