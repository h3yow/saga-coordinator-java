package com.github.h3yow.sagacoordinatorjava.saga.store;

import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;

/**
 * 一条不可变的状态迁移事件（以 JSON 行形式持久化）。
 */
public record SagaEvent(long seq,
                        String sagaId,
                        EventType type,
                        Integer stepIndex,
                        String detail,
                        FailureReason reason,
                        long timestamp) {

    public static SagaEvent of(String sagaId, EventType type, Integer stepIndex,
                               String detail, FailureReason reason) {
        return new SagaEvent(0L, sagaId, type, stepIndex, detail,
                reason == null ? FailureReason.NONE : reason, System.currentTimeMillis());
    }

    public SagaEvent withSeq(long seq) {
        return new SagaEvent(seq, sagaId, type, stepIndex, detail, reason, timestamp);
    }
}
