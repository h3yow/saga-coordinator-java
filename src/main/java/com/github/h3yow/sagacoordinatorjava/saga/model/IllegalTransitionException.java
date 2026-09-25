package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 非法状态迁移：状态机拒绝该迁移，绝不就地改写状态。
 */
public class IllegalTransitionException extends SagaException {

    public IllegalTransitionException(String message) {
        super(message);
    }
}
