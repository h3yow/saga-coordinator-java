package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * SAGA_CREATED 事件 detail 中持久化的定义元数据（只含可重建信息，
 * 不含参与者运行时实例）。
 */
public record DefinitionEnvelope(String type,
                                 List<String> stepNames,
                                 long stepTimeoutMillis,
                                 long sagaTimeoutMillis,
                                 int compensationRetries) {

    @JsonCreator
    public DefinitionEnvelope(@JsonProperty("type") String type,
                              @JsonProperty("stepNames") List<String> stepNames,
                              @JsonProperty("stepTimeoutMillis") long stepTimeoutMillis,
                              @JsonProperty("sagaTimeoutMillis") long sagaTimeoutMillis,
                              @JsonProperty("compensationRetries") int compensationRetries) {
        this.type = type;
        this.stepNames = List.copyOf(stepNames);
        this.stepTimeoutMillis = stepTimeoutMillis;
        this.sagaTimeoutMillis = sagaTimeoutMillis;
        this.compensationRetries = compensationRetries;
    }
}
