package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.InterventionRejectedException;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.InterventionAction;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 人工介入处置：对 COMPENSATION_FAILED 事务重试补偿 / 标记完结；
 * 重复处置幂等返回同一结论；不该处置的状态一律拒绝；结论重启后不漂移。
 */
class ManualInterventionTest {

    private static final Logger log = LoggerFactory.getLogger(ManualInterventionTest.class);

    @TempDir
    Path tempDir;

    /** 制造一个补偿无法收敛、进入 COMPENSATION_FAILED 的事务。 */
    private static TestContext stuckContext() {
        TestContext ctx = new TestContext();
        ctx.failStep = TestSagaFactory.STEP_C;
        ctx.compensationFailStep = TestSagaFactory.STEP_B;
        return ctx;
    }

    private String newStuckTransaction(SagaCoordinator coordinator, TestContext ctx, String txId) {
        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, ctx);
        assertEquals(TransactionStatus.COMPENSATION_FAILED, result.getStatus());
        return txId;
    }

    @Test
    void markResolvedIsIdempotentAndStableAcrossRestart() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        String txId = newStuckTransaction(coordinator, stuckContext(), "tx-op-resolve-001");

        TransactionResult resolved = coordinator.intervene(txId, InterventionAction.MARK_RESOLVED);
        log.info("tx={} after MARK_RESOLVED status={}", txId, resolved.getStatus());
        resolved.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, resolved.getStatus());

        // 重复处置：幂等返回同一结论，不再改写状态
        TransactionResult again = coordinator.intervene(txId, InterventionAction.MARK_RESOLVED);
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, again.getStatus());
        assertEquals(resolved.getTrace().size(), again.getTrace().size(),
                "idempotent re-intervention must not append new events");

        // 重启（新协调器实例指向同一目录）后结论不漂移
        SagaCoordinator restarted = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        TransactionResult reread = restarted.getResult(txId).orElseThrow();
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, reread.getStatus());
        TransactionResult reIntervened = restarted.intervene(txId, InterventionAction.MARK_RESOLVED);
        assertEquals(TransactionStatus.MANUALLY_RESOLVED, reIntervened.getStatus());
        log.info("tx={} conclusion stable across restart: {}", txId, reIntervened.getStatus());
    }

    @Test
    void retryCompensationConvergesWhenParticipantRecovers() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        TestContext ctx = stuckContext();
        String txId = newStuckTransaction(coordinator, ctx, "tx-op-retry-001");

        // 参与者恢复（不再注入补偿失败）后，人工再试一次补偿收尾
        ctx.compensationFailStep = null;
        TransactionResult result = coordinator.intervene(txId, InterventionAction.RETRY_COMPENSATION);
        log.info("tx={} after RETRY_COMPENSATION status={} compensatedSteps={}",
                txId, result.getStatus(), ctx.compensatedSteps);
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));
        assertEquals(TransactionStatus.COMPENSATED, result.getStatus());
        assertEquals(FailureReason.COMPENSATION_FAILED, result.getFailureReason(),
                "failure reason records why it once needed intervention");

        // 重启后结论保持
        SagaCoordinator restarted = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        assertEquals(TransactionStatus.COMPENSATED,
                restarted.getResult(txId).orElseThrow().getStatus());
    }

    @Test
    void retryCompensationKeepsSameConclusionWhileStillFailing() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        TestContext ctx = stuckContext();
        String txId = newStuckTransaction(coordinator, ctx, "tx-op-retry-fail-001");

        // 补偿仍失败：反复处置都收敛到同一个待人工结论
        TransactionResult first = coordinator.intervene(txId, InterventionAction.RETRY_COMPENSATION);
        TransactionResult second = coordinator.intervene(txId, InterventionAction.RETRY_COMPENSATION);
        log.info("tx={} retry#1={} retry#2={}", txId, first.getStatus(), second.getStatus());
        assertEquals(TransactionStatus.COMPENSATION_FAILED, first.getStatus());
        assertEquals(TransactionStatus.COMPENSATION_FAILED, second.getStatus());
        assertEquals(FailureReason.COMPENSATION_FAILED, second.getFailureReason());
    }

    @Test
    void interventionRejectedForIneligibleStates() throws Exception {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);

        // 已完成的事务：两种处置都拒绝
        coordinator.submit(TestSagaFactory.threeStepDefinition(), "tx-op-done-001", new TestContext());
        assertThrows(InterventionRejectedException.class,
                () -> coordinator.intervene("tx-op-done-001", InterventionAction.MARK_RESOLVED));
        assertThrows(InterventionRejectedException.class,
                () -> coordinator.intervene("tx-op-done-001", InterventionAction.RETRY_COMPENSATION));

        // 不存在的事务：拒绝
        assertThrows(InterventionRejectedException.class,
                () -> coordinator.intervene("tx-op-missing-001", InterventionAction.MARK_RESOLVED));

        // 已人工完结的事务：不允许再重试补偿（人已关掉）
        String stuckTx = newStuckTransaction(coordinator, stuckContext(), "tx-op-closed-001");
        coordinator.intervene(stuckTx, InterventionAction.MARK_RESOLVED);
        assertThrows(InterventionRejectedException.class,
                () -> coordinator.intervene(stuckTx, InterventionAction.RETRY_COMPENSATION));

        // 仍在推进中的事务：拒绝处置
        BlockingContext blocking = new BlockingContext();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-op-running-001", blocking));
        assertTrue(blocking.entered.await(10, TimeUnit.SECONDS));
        assertThrows(InterventionRejectedException.class,
                () -> coordinator.intervene("tx-op-running-001", InterventionAction.MARK_RESOLVED));
        log.info("tx=tx-op-running-001 intervention rejected while RUNNING (as expected)");
        blocking.gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        // 被拒绝的处置不能留下任何状态漂移
        assertEquals(TransactionStatus.COMPLETED,
                coordinator.getResult("tx-op-done-001").orElseThrow().getStatus());
        assertEquals(TransactionStatus.COMPLETED,
                coordinator.getResult("tx-op-running-001").orElseThrow().getStatus());
    }

    static class BlockingContext extends TestContext {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);

        @Override
        public void execute(String step) {
            if (TestSagaFactory.STEP_A.equals(step)) {
                entered.countDown();
                while (gate.getCount() > 0) {
                    try {
                        gate.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        // 忽略中断
                    }
                }
            }
            super.execute(step);
        }
    }
}
