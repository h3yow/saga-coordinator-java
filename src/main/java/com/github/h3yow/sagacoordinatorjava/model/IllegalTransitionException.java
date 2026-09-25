package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 非法状态迁移：拒绝执行而不是就地改写。
 */
public class IllegalTransitionException extends RuntimeException {

    public IllegalTransitionException(String message) {
        super(message);
    }
}
