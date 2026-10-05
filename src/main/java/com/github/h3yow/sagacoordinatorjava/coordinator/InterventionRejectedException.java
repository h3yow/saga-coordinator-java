package com.github.h3yow.sagacoordinatorjava.coordinator;

/**
 * 人工介入处置被拒绝：事务不存在、仍在推进、或已到不允许再处置的终态。
 */
public class InterventionRejectedException extends RuntimeException {

    public InterventionRejectedException(String message) {
        super(message);
    }
}
