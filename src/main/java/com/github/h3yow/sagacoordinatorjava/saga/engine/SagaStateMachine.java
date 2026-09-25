package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.IllegalTransitionException;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;

import java.util.Map;
import java.util.Set;

/**
 * 事务状态机：集中声明合法迁移，任何非法迁移直接抛
 * {@link IllegalTransitionException}，绝不就地改写。
 */
public final class SagaStateMachine {

    private static final Map<SagaState, Set<SagaState>> ALLOWED = Map.of(
            SagaState.STARTED, Set.of(SagaState.RUNNING, SagaState.FAILED),
            SagaState.RUNNING, Set.of(SagaState.COMPLETED, SagaState.COMPENSATING,
                    SagaState.FAILED, SagaState.TIMED_OUT, SagaState.MANUAL_INTERVENTION),
            SagaState.COMPENSATING, Set.of(SagaState.COMPENSATED,
                    SagaState.COMPENSATION_FAILED, SagaState.TIMED_OUT, SagaState.MANUAL_INTERVENTION),
            SagaState.COMPLETED, Set.of(),
            SagaState.COMPENSATED, Set.of(),
            SagaState.COMPENSATION_FAILED, Set.of(),
            SagaState.FAILED, Set.of(),
            SagaState.TIMED_OUT, Set.of(SagaState.MANUAL_INTERVENTION),
            SagaState.MANUAL_INTERVENTION, Set.of()
    );

    private SagaStateMachine() {
    }

    public static boolean canTransit(SagaState from, SagaState to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    public static void requireTransit(SagaState from, SagaState to) {
        if (!canTransit(from, to)) {
            throw new IllegalTransitionException(
                    "非法状态迁移: " + from + " -> " + to);
        }
    }
}
