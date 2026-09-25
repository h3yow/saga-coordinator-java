package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 协调器内部不可恢复错误（如存储损坏）。
 */
public class SagaException extends RuntimeException {

    public SagaException(String message) {
        super(message);
    }

    public SagaException(String message, Throwable cause) {
        super(message, cause);
    }
}
