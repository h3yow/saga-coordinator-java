package com.github.h3yow.sagacoordinatorjava.coordinator;

/**
 * 并发容量已满且策略为 REJECT：新事务提交被快速拒绝，
 * 不占用线程、不排队等待。调用方可稍后重试。
 */
public class CapacityExceededException extends RuntimeException {

    public CapacityExceededException(String message) {
        super(message);
    }
}
