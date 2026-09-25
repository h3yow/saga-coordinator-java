package com.github.h3yow.sagacoordinatorjava.saga.model;

import java.time.Duration;
import java.util.List;

/**
 * 一个事务编排定义：有序步骤与超时/补偿策略。
 *
 * @param type                事务类型名（同名定义的步骤必须一致）
 * @param steps               按正向提交顺序排列的参与者步骤
 * @param stepTimeout         单个步骤的执行/补偿超时
 * @param sagaTimeout         整个事务的总超时
 * @param compensationRetries 单个步骤补偿失败后的重试次数（不含首次）
 */
public record SagaDefinition(String type,
                             List<SagaStep> steps,
                             Duration stepTimeout,
                             Duration sagaTimeout,
                             int compensationRetries) {

    public SagaDefinition {
        steps = List.copyOf(steps);
    }

    public static Builder builder(String type) {
        return new Builder(type);
    }

    public static final class Builder {
        private final String type;
        private List<SagaStep> steps = List.of();
        private Duration stepTimeout = Duration.ofSeconds(5);
        private Duration sagaTimeout = Duration.ofSeconds(30);
        private int compensationRetries = 1;

        private Builder(String type) {
            this.type = type;
        }

        public Builder steps(List<SagaStep> steps) {
            this.steps = steps;
            return this;
        }

        public Builder stepTimeout(Duration stepTimeout) {
            this.stepTimeout = stepTimeout;
            return this;
        }

        public Builder sagaTimeout(Duration sagaTimeout) {
            this.sagaTimeout = sagaTimeout;
            return this;
        }

        public Builder compensationRetries(int compensationRetries) {
            this.compensationRetries = compensationRetries;
            return this;
        }

        public SagaDefinition build() {
            return new SagaDefinition(type, steps, stepTimeout, sagaTimeout, compensationRetries);
        }
    }
}
