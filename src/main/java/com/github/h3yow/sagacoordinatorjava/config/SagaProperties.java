package com.github.h3yow.sagacoordinatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Saga 协调器配置。
 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    /** 事务状态文件目录 */
    private String storeDir = "build/saga-store";

    /** 单步执行超时（毫秒），0 表示不限制 */
    private long stepTimeoutMillis = 2000;

    /** 事务级超时：超过该时长无进展即判定超时（毫秒） */
    private long transactionTimeoutMillis = 30000;

    /** 补偿最大重试次数 */
    private int compensationMaxRetries = 3;

    /** 同时推进中的事务数量上限；0 表示不限制 */
    private int maxConcurrentTransactions = 50;

    /** 超出上限时的策略：QUEUE（排队等待）或 REJECT（快速拒绝） */
    private String capacityPolicy = "QUEUE";

    /** QUEUE 策略下排队等待许可的最长时间（毫秒），超时拒绝；0 表示一直等 */
    private long capacityAcquireTimeoutMillis = 10_000;

    public String getStoreDir() {
        return storeDir;
    }

    public void setStoreDir(String storeDir) {
        this.storeDir = storeDir;
    }

    public long getStepTimeoutMillis() {
        return stepTimeoutMillis;
    }

    public void setStepTimeoutMillis(long stepTimeoutMillis) {
        this.stepTimeoutMillis = stepTimeoutMillis;
    }

    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    public void setTransactionTimeoutMillis(long transactionTimeoutMillis) {
        this.transactionTimeoutMillis = transactionTimeoutMillis;
    }

    public int getCompensationMaxRetries() {
        return compensationMaxRetries;
    }

    public void setCompensationMaxRetries(int compensationMaxRetries) {
        this.compensationMaxRetries = compensationMaxRetries;
    }

    public int getMaxConcurrentTransactions() {
        return maxConcurrentTransactions;
    }

    public void setMaxConcurrentTransactions(int maxConcurrentTransactions) {
        this.maxConcurrentTransactions = maxConcurrentTransactions;
    }

    public String getCapacityPolicy() {
        return capacityPolicy;
    }

    public void setCapacityPolicy(String capacityPolicy) {
        this.capacityPolicy = capacityPolicy;
    }

    public long getCapacityAcquireTimeoutMillis() {
        return capacityAcquireTimeoutMillis;
    }

    public void setCapacityAcquireTimeoutMillis(long capacityAcquireTimeoutMillis) {
        this.capacityAcquireTimeoutMillis = capacityAcquireTimeoutMillis;
    }
}
