package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 单个步骤的持久化状态。
 */
public class StepRecord {

    private String name;
    private StepStatus status = StepStatus.INIT;
    private int attempts;
    private long updatedAt;
    private String lastError;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
}
