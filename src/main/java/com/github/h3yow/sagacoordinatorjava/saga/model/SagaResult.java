package com.github.h3yow.sagacoordinatorjava.saga.model;

import java.util.Map;

/**
 * 协调器对一次提交/查询返回的不可变结论视图。
 * 重复或并发提交同一 sagaId 会得到相同结论。
 */
public record SagaResult(String sagaId,
                         String type,
                         SagaState state,
                         FailureReason reason,
                         String reasonDetail,
                         int currentStepIndex,
                         Map<String, String> context) {
}
