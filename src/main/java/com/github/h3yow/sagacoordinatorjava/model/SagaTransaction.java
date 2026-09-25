package com.github.h3yow.sagacoordinatorjava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Saga 事务实例的持久化聚合根。
 * 所有字段均可被 Jackson 序列化到本地文件。
 */
public class SagaTransaction {

    private String transactionId;
    private String sagaType;
    private TransactionStatus status = TransactionStatus.PENDING;
    private FailureReason failureReason = FailureReason.NONE;
    private String failureDetail;
    private int currentStepIndex;
    private long createdAt;
    private long updatedAt;
    private long deadlineAt;
    private int compensationRetries;
    private List<StepRecord> steps = new ArrayList<>();
    private List<TransactionEvent> events = new ArrayList<>();

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getSagaType() {
        return sagaType;
    }

    public void setSagaType(String sagaType) {
        this.sagaType = sagaType;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public FailureReason getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(FailureReason failureReason) {
        this.failureReason = failureReason;
    }

    public String getFailureDetail() {
        return failureDetail;
    }

    public void setFailureDetail(String failureDetail) {
        this.failureDetail = failureDetail;
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public void setCurrentStepIndex(int currentStepIndex) {
        this.currentStepIndex = currentStepIndex;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public long getDeadlineAt() {
        return deadlineAt;
    }

    public void setDeadlineAt(long deadlineAt) {
        this.deadlineAt = deadlineAt;
    }

    public int getCompensationRetries() {
        return compensationRetries;
    }

    public void setCompensationRetries(int compensationRetries) {
        this.compensationRetries = compensationRetries;
    }

    public List<StepRecord> getSteps() {
        return steps;
    }

    public void setSteps(List<StepRecord> steps) {
        this.steps = steps;
    }

    public List<TransactionEvent> getEvents() {
        return events;
    }

    public void setEvents(List<TransactionEvent> events) {
        this.events = events;
    }
}
