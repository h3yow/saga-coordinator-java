package com.github.h3yow.sagacoordinatorjava.coordinator;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 事务推进准入控制：限制同一时间正在推进（提交 / 恢复 / 补偿收尾）的事务数量。
 * 超出上限的请求在队列超时时间内排队等待，仍拿不到许可则拒绝，
 * 避免个别挂起事务把线程与资源占满。
 */
public class AdmissionController {

    private final int maxConcurrent;
    private final long queueTimeoutMillis;
    private final Semaphore permits;

    /**
     * @param maxConcurrent       同时推进的事务上限，&lt;=0 表示不限制
     * @param queueTimeoutMillis  排队等待许可的最长时间（毫秒）
     */
    public AdmissionController(int maxConcurrent, long queueTimeoutMillis) {
        this.maxConcurrent = maxConcurrent;
        this.queueTimeoutMillis = queueTimeoutMillis;
        this.permits = maxConcurrent > 0 ? new Semaphore(maxConcurrent) : null;
    }

    /**
     * 尝试获取一个推进许可；拿到后必须配对调用 {@link #release()}。
     *
     * @throws ConcurrencyLimitExceededException 排队超时仍无许可
     */
    public void acquire(String transactionId) {
        if (permits == null) {
            return;
        }
        try {
            if (!permits.tryAcquire(queueTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new ConcurrencyLimitExceededException(
                        "too many in-flight transactions (limit=" + maxConcurrent
                                + "), admission timed out after " + queueTimeoutMillis + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConcurrencyLimitExceededException("interrupted while waiting for admission");
        }
    }

    /**
     * 立即尝试获取许可（巡检等内部场景：拿不到就跳过本轮，不排队）。
     *
     * @return true 表示拿到许可，必须配对调用 {@link #release()}
     */
    public boolean tryAcquire() {
        return permits == null || permits.tryAcquire();
    }

    public void release() {
        if (permits != null) {
            permits.release();
        }
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }
}
