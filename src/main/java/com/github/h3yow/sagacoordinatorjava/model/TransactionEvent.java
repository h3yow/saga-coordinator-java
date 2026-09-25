package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 状态迁移轨迹中的一条不可变事件。
 */
public class TransactionEvent {

    private long timestamp;
    private EventType type;
    private String step;
    private String detail;

    public TransactionEvent() {
    }

    public TransactionEvent(long timestamp, EventType type, String step, String detail) {
        this.timestamp = timestamp;
        this.type = type;
        this.step = step;
        this.detail = detail;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    @Override
    public String toString() {
        return String.format("[%d] %s%s%s", timestamp, type,
                step != null ? " step=" + step : "",
                detail != null ? " (" + detail + ")" : "");
    }
}
