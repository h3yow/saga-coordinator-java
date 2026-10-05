package com.github.h3yow.sagacoordinatorjava.model;

import java.util.Map;
import java.util.Set;

/**
 * 事务与步骤的合法状态迁移表。
 * 任何不在表内的迁移都抛 {@link IllegalTransitionException}，拒绝就地改写。
 */
public final class StateMachine {

    private static final Map<TransactionStatus, Set<TransactionStatus>> TX_ALLOWED = Map.of(
            TransactionStatus.PENDING, Set.of(TransactionStatus.RUNNING),
            TransactionStatus.RUNNING, Set.of(
                    TransactionStatus.COMPLETED,
                    TransactionStatus.COMPENSATING,
                    TransactionStatus.FAILED,
                    TransactionStatus.TIMEOUT_COMPENSATING),
            TransactionStatus.COMPENSATING, Set.of(
                    TransactionStatus.COMPENSATED,
                    TransactionStatus.COMPENSATION_FAILED,
                    TransactionStatus.TIMEOUT_COMPENSATING),
            TransactionStatus.TIMEOUT_COMPENSATING, Set.of(
                    TransactionStatus.TIMED_OUT_COMPENSATED,
                    TransactionStatus.COMPENSATION_FAILED),
            // 人工介入：补偿未收敛的事务可被重试补偿，或被显式标记为人工已完结
            TransactionStatus.COMPENSATION_FAILED, Set.of(
                    TransactionStatus.COMPENSATING,
                    TransactionStatus.MANUALLY_RESOLVED)
            // 终态 COMPLETED / COMPENSATED / FAILED /
            // MANUALLY_RESOLVED / TIMED_OUT_COMPENSATED 无后续迁移
    );

    private static final Map<StepStatus, Set<StepStatus>> STEP_ALLOWED = Map.of(
            StepStatus.INIT, Set.of(StepStatus.EXECUTING),
            StepStatus.EXECUTING, Set.of(
                    StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.TIMED_OUT),
            StepStatus.COMPLETED, Set.of(StepStatus.COMPENSATING),
            StepStatus.COMPENSATING, Set.of(StepStatus.COMPENSATED)
            // COMPENSATING 下重试不改变状态；失败终态无后续迁移
    );

    private StateMachine() {
    }

    public static void assertTxTransition(TransactionStatus from, TransactionStatus to) {
        if (from == to) {
            return;
        }
        if (!TX_ALLOWED.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalTransitionException(
                    "illegal transaction transition: " + from + " -> " + to);
        }
    }

    public static void assertStepTransition(StepStatus from, StepStatus to) {
        if (from == to) {
            return;
        }
        if (!STEP_ALLOWED.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalTransitionException(
                    "illegal step transition: " + from + " -> " + to);
        }
    }

    public static boolean isTerminal(TransactionStatus status) {
        return status == TransactionStatus.COMPLETED
                || status == TransactionStatus.COMPENSATED
                || status == TransactionStatus.FAILED
                || status == TransactionStatus.COMPENSATION_FAILED
                || status == TransactionStatus.TIMED_OUT_COMPENSATED
                || status == TransactionStatus.MANUALLY_RESOLVED;
    }
}
