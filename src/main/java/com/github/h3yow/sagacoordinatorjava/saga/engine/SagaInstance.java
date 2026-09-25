package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个事务实例的内存聚合：由事件重放得到，状态机迁移的唯一持有者。
 */
public final class SagaInstance {

    private final String sagaId;
    private final SagaDefinition definition;
    private final SagaContext context;
    private final List<StepStateRecord> stepRecords;

    private SagaState state = SagaState.STARTED;
    private FailureReason reason = FailureReason.NONE;
    private String reasonDetail = "";
    private int currentStepIndex;
    private int compensationFromIndex = -1;
    private long createdAt = System.currentTimeMillis();
    private long updatedAt = createdAt;

    public SagaInstance(String sagaId, SagaDefinition definition, SagaContext context) {
        this.sagaId = sagaId;
        this.definition = definition;
        this.context = context;
        this.stepRecords = new ArrayList<>();
        for (int i = 0; i < definition.steps().size(); i++) {
            this.stepRecords.add(new StepStateRecord());
        }
    }

    public String getSagaId() {
        return sagaId;
    }

    public SagaDefinition getDefinition() {
        return definition;
    }

    public SagaContext getContext() {
        return context;
    }

    public List<StepStateRecord> getStepRecords() {
        return stepRecords;
    }

    public SagaState getState() {
        return state;
    }

    public void setState(SagaState state) {
        this.state = state;
        this.updatedAt = System.currentTimeMillis();
    }

    public FailureReason getReason() {
        return reason;
    }

    public void setReason(FailureReason reason) {
        this.reason = reason;
    }

    public String getReasonDetail() {
        return reasonDetail;
    }

    public void setReasonDetail(String reasonDetail) {
        this.reasonDetail = reasonDetail == null ? "" : reasonDetail;
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public void setCurrentStepIndex(int currentStepIndex) {
        this.currentStepIndex = currentStepIndex;
    }

    public int getCompensationFromIndex() {
        return compensationFromIndex;
    }

    public void setCompensationFromIndex(int compensationFromIndex) {
        this.compensationFromIndex = compensationFromIndex;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public boolean isTerminal() {
        return switch (state) {
            case COMPLETED, COMPENSATED, COMPENSATION_FAILED, FAILED, TIMED_OUT, MANUAL_INTERVENTION -> true;
            default -> false;
        };
    }
}
