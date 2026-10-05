package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.ManualActionRejectedException;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 人工介入处置：
 * 1) 对 COMPENSATION_FAILED 事务再试一次补偿可收敛；
 * 2) 重复处置幂等、返回同一结论；
 * 3) 已结束 / 不该再动的事务拒绝处置；
 * 4) 人工标记 MANUALLY_RESOLVED 后重启结论不漂移。
 */
class ManualInterventionTest {

    private static final Logger log = LoggerFactory.getLogger(ManualInterventionTest.class);

    @TempDir
    Path tempDir;

    /** 先制造一个 COMPENSATION_FAILED：C 业务失败，B 的补偿一直失败。 */
    private String givenCompensationFailed(String txId) {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 1);
        TestContext context = new TestContext();
        context.failStep = TestSagaFactory.STEP_C;
        context.compensationFailStep = TestSagaFactory.STEP_B;
        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);
        assertEquals(TransactionStatus.COMPENSATION_FAILED, result.getStatus());
        log.info("tx={} prepared status={} detail={}",
                txId, result.getStatus(), result.getFailureDetail());
        return txId;
    }

    @Test
    void manualRetryConvergesAndIsIdempotent() {
        String txId = givenCompensationFailed("tx-manual-retry-001");

        SagaCoordinator ops = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        // 人工修复参与者后，新上下文里补偿不再失败
        TestContext fixed = new TestContext();
        ops.register(TestSagaFactory.threeStepDefinition(), () -> fixed);

        TransactionResult retried = ops.retryCompensation(txId);
        log.info("tx={} after manual retry status={}", txId, retried.getStatus());
        retried.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPENSATED, retried.getStatus());
        // 逆序补偿：C 无业务效果跳过，只补 B、A
        assertEquals(1, fixed.compensateCount(TestSagaFactory.STEP_B));
        assertEquals(1, fixed.compensateCount(TestSagaFactory.STEP_A));

        // 重复再试：幂等返回同一结论，不再产生业务效果
        TransactionResult again = ops.retryCompensation(txId);
        assertEquals(TransactionStatus.COMPENSATED, again.getStatus());
        assertEquals(1, fixed.compensateCount(TestSagaFactory.STEP_B));
    }

    @Test
    void retryRejectedForWrongStatuses() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        String txId = "tx-manual-ok-001";
        TransactionResult ok = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);
        assertEquals(TransactionStatus.COMPLETED, ok.getStatus());

        // 已正常结束的事务不允许重试补偿
        ManualActionRejectedException ex = assertThrows(ManualActionRejectedException.class,
                () -> coordinator.retryCompensation(txId));
        assertTrue(ex.getMessage().contains(txId));
        log.info("retry on completed rejected: {}", ex.getMessage());

        // 不存在的事务直接报错
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.retryCompensation("tx-missing"));
    }

    @Test
    void manualResolveIsIdempotentAndRejectsOtherStatuses() {
        String txId = givenCompensationFailed("tx-manual-resolve-001");
        SagaCoordinator ops = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);

        TransactionResult resolved = ops.resolveManually(txId, "balance fixed by ops #42");
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, resolved.getStatus());
        assertEquals(FailureReason.MANUALLY_RESOLVED, resolved.getFailureReason());

        // 重复标记：幂等
        TransactionResult again = ops.resolveManually(txId, "second call");
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, again.getStatus());
        assertEquals(FailureReason.MANUALLY_RESOLVED, again.getFailureReason());

        // 人工处理完毕后再试补偿：终态幂等返回同一结论，不会再动事务
        TransactionResult retryAfterResolve = ops.retryCompensation(txId);
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, retryAfterResolve.getStatus());
        log.info("retry after resolution idempotently returns {}",
                retryAfterResolve.getStatus());

        // 重启（新协调器指向同一目录）后结论不漂移
        SagaCoordinator restarted = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TransactionResult afterRestart = restarted.getResult(txId).orElseThrow();
        log.info("tx={} after restart status={} reason={}",
                txId, afterRestart.getStatus(), afterRestart.getFailureReason());
        afterRestart.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, afterRestart.getStatus());
        assertEquals(FailureReason.MANUALLY_RESOLVED, afterRestart.getFailureReason());
    }

    @Test
    void resolveRejectedForRunningTransaction() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        String txId = "tx-manual-running-001";
        coordinator.submit(TestSagaFactory.threeStepDefinition(), txId, context);

        // 已正常 COMPLETED 的事务不能被标记人工处理
        ManualActionRejectedException ex = assertThrows(ManualActionRejectedException.class,
                () -> coordinator.resolveManually(txId, "nope"));
        log.info("resolve on completed rejected: {}", ex.getMessage());
    }
}
