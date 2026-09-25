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
    RECOVERY_TIMEOUT
}
