package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 事务总览列表中的一行：只包含运维分辨事务所需的摘要字段，
 * 不含步骤明细与完整事件轨迹（后者用单事务详情接口获取）。
 *
 * <p>对象很小，即使事务累积到上千条，分页查询也只需在内存中持有摘要，
 * 而不是完整的事务聚合（含不断增长的事件轨迹）。
 */
public class TransactionSummary {

    private String transactionId;
    private String sagaType;
    private TransactionStatus status;
    private FailureReason failureReason;
    private String failureDetail;
    private long createdAt;
    /** 最近一次进展时间（任何状态迁移 / 步骤推进都会刷新）。 */
    private long updatedAt;

    public TransactionSummary() {
    }

    public TransactionSummary(String transactionId, String sagaType, TransactionStatus status,
                              FailureReason failureReason, String failureDetail,
                              long createdAt, long updatedAt) {
        this.transactionId = transactionId;
        this.sagaType = sagaType;
        this.status = status;
        this.failureReason = failureReason;
        this.failureDetail = failureDetail;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

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
}
