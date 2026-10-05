package com.github.h3yow.sagacoordinatorjava.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 同时推进中的事务数量上限。每个事务从开始驱动到终态占用一个许可，
 * 超出上限时按策略处理：
 * <ul>
 *     <li>{@code QUEUE}：阻塞等待，直到拿到许可或超过排队等待上限；</li>
 *     <li>{@code REJECT}：不等待（或仅极短等待），直接快速拒绝。</li>
 * </ul>
 *
 * <p>调用方拿到 {@link Permit} 后必须 try/finally 释放；
 * permit<=0 表示不限制（{@link #unlimited()}）。
 */
public class ConcurrencyLimiter {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyLimiter.class);

    /** 容量满时的处置策略。 */
    public enum Policy { QUEUE, REJECT }

    private final int maxPermits;
    private final Policy policy;
    private final long acquireTimeoutMillis;
    private final Semaphore semaphore;

    private ConcurrencyLimiter(int maxPermits, Policy policy, long acquireTimeoutMillis) {
        this.maxPermits = maxPermits;
        this.policy = policy;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.semaphore = maxPermits > 0 ? new Semaphore(maxPermits, true) : null;
    }

    public static ConcurrencyLimiter of(int maxPermits, Policy policy, long acquireTimeoutMillis) {
        return new ConcurrencyLimiter(maxPermits, policy, acquireTimeoutMillis);
    }

    public static ConcurrencyLimiter unlimited() {
        return new ConcurrencyLimiter(0, Policy.QUEUE, 0);
    }

    /**
     * 申请一个事务推进许可。
     *
     * @throws CapacityExceededException REJECT 策略下拿不到许可，或 QUEUE 等待超时
     */
    public Permit acquire(String transactionId) {
        if (semaphore == null) {
            return Permit.noop();
        }
        long waitMillis = policy == Policy.REJECT ? 0L : Math.max(acquireTimeoutMillis, 0L);
        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CapacityExceededException(
                    "interrupted while waiting for capacity, tx=" + transactionId);
        }
        if (!acquired) {
            int inFlight = maxPermits - semaphore.availablePermits();
            log.warn("tx={} capacity exceeded (policy={}, maxConcurrent={}, inFlight={})",
                    transactionId, policy, maxPermits, inFlight);
            throw new CapacityExceededException("saga concurrency limit " + maxPermits
                    + " reached (policy=" + policy + "), tx=" + transactionId);
        }
        return new SemaphorePermit(semaphore, transactionId);
    }

    public int availablePermits() {
        return semaphore == null ? Integer.MAX_VALUE : semaphore.availablePermits();
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    /** 许可句柄，离开作用域必须释放。 */
    public interface Permit extends AutoCloseable {
        @Override
        void close();

        static Permit noop() {
            return () -> { };
        }
    }

    private static final class SemaphorePermit implements Permit {
        private final Semaphore semaphore;
        private final String transactionId;

        private SemaphorePermit(Semaphore semaphore, String transactionId) {
            this.semaphore = semaphore;
            this.transactionId = transactionId;
        }

        @Override
        public void close() {
            semaphore.release();
            log.debug("tx={} permit released", transactionId);
        }
    }
}
