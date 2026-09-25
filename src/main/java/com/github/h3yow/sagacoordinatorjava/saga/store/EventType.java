package com.github.h3yow.sagacoordinatorjava.saga.store;

/**
 * 事件溯源日志中的事件类型。每次状态迁移都先落盘事件，再更新内存态。
 */
public enum EventType {
    SAGA_CREATED,
    SAGA_COMPENSATION_STARTED,
    STEP_STARTED,
    STEP_COMPLETED,
    STEP_FAILED,
    STEP_TIMED_OUT,
    STEP_COMPENSATION_STARTED,
    STEP_COMPENSATED,
    STEP_COMPENSATION_FAILED,
    SAGA_FINISHED
}
