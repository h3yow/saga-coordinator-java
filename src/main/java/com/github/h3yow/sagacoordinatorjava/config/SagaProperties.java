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
}
