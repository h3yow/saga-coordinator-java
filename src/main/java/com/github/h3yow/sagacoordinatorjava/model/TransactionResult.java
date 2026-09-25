package com.github.h3yow.sagacoordinatorjava.model;

import java.util.List;

/**
 * 提交 / 查询返回给调用方的可解释结论。
 * 重复提交同一事务将得到同一个结论。
 */
public class TransactionResult {

    private String transactionId;
    private TransactionStatus status;
    private FailureReason failureReason;
    private String failureDetail;
    private List<TransactionEvent> trace;

    public TransactionResult() {
    }

    public TransactionResult(String transactionId, TransactionStatus status,
                             FailureReason failureReason, String failureDetail,
                             List<TransactionEvent> trace) {
        this.transactionId = transactionId;
        this.status = status;
        this.failureReason = failureReason;
        this.failureDetail = failureDetail;
        this.trace = trace;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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

    public List<TransactionEvent> getTrace() {
        return trace;
    }

    public void setTrace(List<TransactionEvent> trace) {
        this.trace = trace;
    }
}
