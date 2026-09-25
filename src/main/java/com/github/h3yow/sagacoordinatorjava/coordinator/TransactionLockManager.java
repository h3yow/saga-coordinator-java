package com.github.h3yow.sagacoordinatorjava.coordinator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 按事务 ID 分段加锁：同一事务的并发提交串行化，
 * 不同事务互不阻塞，超时巡检因此不会卡住其它事务。
 */
public class TransactionLockManager {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public ReentrantLock get(String transactionId) {
        return locks.computeIfAbsent(transactionId, k -> new ReentrantLock());
    }
}
