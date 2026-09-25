package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.StepState;

/**
 * 单个步骤在聚合内的可变状态记录。
 */
public final class StepStateRecord {

    private StepState state = StepState.PENDING;
    private int attempts;
    private int compensationAttempts;
    private long lastUpdateMillis;
    private String lastMessage = "";

    public StepState getState() {
        return state;
    }

    public void setState(StepState state) {
        this.state = state;
        this.lastUpdateMillis = System.currentTimeMillis();
    }

    public int getAttempts() {
        return attempts;
    }

    public void incAttempts() {
        this.attempts++;
        this.lastUpdateMillis = System.currentTimeMillis();
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public void incCompensationAttempts() {
        this.compensationAttempts++;
        this.lastUpdateMillis = System.currentTimeMillis();
    }

    public long getLastUpdateMillis() {
        return lastUpdateMillis;
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public void setLastMessage(String lastMessage) {
        this.lastMessage = lastMessage;
    }
}
