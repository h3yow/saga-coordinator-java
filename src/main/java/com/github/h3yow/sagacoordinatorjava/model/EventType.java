package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 事务事件轨迹中的事件类型，用于审计与日志解释。
 */
public enum EventType {
    CREATED,
    STATE_CHANGED,
    STEP_STARTED,
    STEP_COMPLETED,
    STEP_FAILED,
    STEP_TIMED_OUT,
    COMPENSATION_STARTED,
    STEP_COMPENSATED,
    STEP_COMPENSATION_FAILED,
    RECOVERY_RESUMED,
    RECOVERY_TIMEOUT,
    /** 运维手动再试一次补偿（仅 COMPENSATION_FAILED 事务可发起）。 */
    MANUAL_RETRY_REQUESTED,
    /** 运维显式确认事务残留效果已人工处理完毕。 */
    MANUALLY_RESOLVED
}
