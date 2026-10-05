package com.github.h3yow.sagacoordinatorjava.coordinator;

/**
 * 同时在推进的事务数达到配置上限，且排队等待超时后被拒绝。
 */
public class ConcurrencyLimitExceededException extends RuntimeException {

    public ConcurrencyLimitExceededException(String message) {
        super(message);
    }
}
